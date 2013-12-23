/*
 * Copyright (c) 2013-2014 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.kinesis.producer

// Java
import java.nio.ByteBuffer

// Amazon
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{
  BasicAWSCredentials,
  ClasspathPropertiesFileCredentialsProvider
}

// Scalazon (for Kinesis interaction)
import io.github.cloudify.scala.aws.kinesis.Client
import io.github.cloudify.scala.aws.kinesis.Client.ImplicitExecution._
import io.github.cloudify.scala.aws.kinesis.Definitions.{Stream,PutResult}
import io.github.cloudify.scala.aws.kinesis.KinesisDsl._

// Config
import com.typesafe.config.Config

// SnowPlow Utils
import com.snowplowanalytics.util.Tap._

// Concurrent utilities.
import scala.concurrent.{Future,Await,TimeoutException}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * The core logic for the Kinesis event producer
 */
case class StreamProducer(config: Config) {

  // Grab all the configuration variables one-time
  private object ProducerConfig {

    private val producer = config.getConfig("producer")

    private val aws = producer.getConfig("aws")
    val awsAccessKey = aws.getString("access-key")
    val awsSecretKey = aws.getString("secret-key")

    private val stream = producer.getConfig("stream")
    val streamName = stream.getString("name")
    val streamSize = stream.getInt("size")

    private val events = producer.getConfig("events")
    val eventsOrdered = events.getBoolean("ordered")
    val eventsLimit = {
      val l = events.getInt("limit")
      if (l == 0) None else Some(l)
    }

    private val ap = producer.getConfig("active-polling")
    val apDuration = ap.getInt("duration")
    val apInterval = ap.getInt("interval")
  }

  // Initialize
  private implicit val kinesis = createKinesisClient(ProducerConfig.awsAccessKey, ProducerConfig.awsSecretKey)
  private var stream: Option[Stream] = None

  /**
   * Creates a new stream. Arguments are
   * optional - defaults to the values
   * provided in the ProducerConfig if
   * not provided.
   *
   * @param name The name of the stream
   * to create
   * @param size The number of shards to
   * support for this stream
   * @param duration How long to keep
   * checking if the stream became active,
   * in seconds
   * @param interval How frequently to
   * check if the stream has become active,
   * in seconds
   *
   * @return a Boolean, where:
   * 1. true means the stream became active
   *    while we were polling its status
   * 2. false means the stream did not
   *    become active while we were polling 
   */
  def createStream(
      name: String = ProducerConfig.streamName,
      size: Int = ProducerConfig.streamSize,
      duration: Int = ProducerConfig.apDuration,
      interval: Int = ProducerConfig.apInterval): Boolean = {
    val createStream = for {
      s <- Kinesis.streams.create(name)
    } yield s

    try {
      stream = Some(Await.result(createStream, Duration(duration, SECONDS)))
      Await.result(stream.get.waitActive.retrying(duration),
        Duration(duration, SECONDS))
    } catch {
      case _: TimeoutException => false
    }
    true
  }

  /**
   * Produces an (in)finite stream of events.
   *
   * @param name The name of the stream
   * to produce events for
   * @param ordered Whether the sequence
   * numbers of the events should always be
   * ordered
   * @param limit How many events to produce
   * in this stream. Use None for an infinite
   * stream
   */
  def produceStream(
      name: String = ProducerConfig.streamName,
      ordered: Boolean = ProducerConfig.eventsOrdered,
      limit: Option[Int] = ProducerConfig.eventsLimit) {
    
    if (stream.isEmpty) {
      stream = Some(Kinesis.stream(name))
    }

    def write() = writeExampleRecord(name, System.currentTimeMillis()) // Alias
    (ordered, limit) match {
      case (false, None)    => while (true) { write() }
      case (true,  None)    => throw new RuntimeException("Ordered stream support not yet implemented") // TODO
      case (false, Some(c)) => (1 to c).foreach(_ => write())
      case (true,  Some(c)) => throw new RuntimeException("Ordered stream support not yet implemented") // TODO
    }
  }

  // Debugging function to print all records in the current stream.
  def printRecords() {
    val getRecords = for {
      shards <- stream.get.shards.list
      iterators <- Future.sequence(shards.map {
        shard => implicitExecute(shard.iterator)
      })
      records <- Future.sequence(iterators.map {
        iterator => implicitExecute(iterator.nextRecords)
      })
    } yield records
    val nextRecordIter = Await.result(getRecords, 30.seconds)
    for (nextRecord <- nextRecordIter) {
      for (record <- nextRecord.records) {
        println("sequenceNumber: " + record.sequenceNumber)
        println("data: " + new String(record.data.array()))
        println("partitionKey: " + record.partitionKey)
      }
    }
  }

  /**
   * Creates a new Kinesis client from
   * provided AWS access key and secret
   * key. If both are set to "cpf", then
   * authenticate using the classpath
   * properties file.
   *
   * @return the initialized
   * AmazonKinesisClient
   */
  private[producer] def createKinesisClient(
      accessKey: String, secretKey: String): Client =
    if (isCpf(accessKey) && isCpf(secretKey)) {
      Client.fromCredentials(new ClasspathPropertiesFileCredentialsProvider())
    } else if (isCpf(accessKey) || isCpf(secretKey)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'cpf', or neither of them")
    } else {
      Client.fromCredentials(accessKey, secretKey)
    }

  /**
   * Writes an example record to the given
   * stream. Uses the supplied timestamp
   * to make the record identifiable.
   *
   * @param stream The name of the stream
   * to write the record to
   * @param timestamp When this record was
   * created
   *
   * @return the shard ID this record was
   * written to
   */
  private[producer] def writeExampleRecord(stream: String, timestamp: Long): String =
    writeRecord(
      data = "example-record-%s".format(timestamp),
      key = "partition-key-%s".format(timestamp % 100000)
    )

  /**
   * Writes a record to the given stream
   *
   * @param data The data for this record
   * @param key The partition key for this
   * record
   * @param duration Time in seconds to wait
   * to put the data.
   *
   * @return the shard ID this record was
   * written to
   */
  private[producer] def writeRecord(data: String, key: String,
      duration: Int = ProducerConfig.apDuration): String = {
    val putData = for {
      p <- stream.get.put(ByteBuffer.wrap(data.getBytes), key)
    } yield p
    //val putResult = Await.result(putData,
    //  Duration(duration, SECONDS))
    //putResult.shardId
    //TODO: Return shard ID written to.
    ""
  }

  /**
   * Is the access/secret key set to
   * the special value "cpf" i.e. use
   * the classpath properties file
   * for credentials.
   *
   * @param key The key to check
   *
   * @return true if key is cpf, false
   * otherwise
   */
  private[producer] def isCpf(key: String): Boolean = (key == "cpf")

  /**
   * Is this exception a ResourceNotFoundException?
   *
   * @param ase The AmazonServiceException to check
   *
   * @return true if it's a ResourceNotFoundException,
   * false otherwise
   */
  private[producer] def isResourceNotFoundException(ase: AmazonServiceException): Boolean = 
    ase.getErrorCode.equalsIgnoreCase("ResourceNotFoundException")
}
