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
package com.snowplowanalytics.kinesis.consumer

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
 * The core logic for the Kinesis event consumer.
 */
case class StreamConsumer(config: Config) {

  // Grab all the configuration variables one-time
  private object ConsumerConfig {

    private val consumer = config.getConfig("consumer")

    private val aws = consumer.getConfig("aws")
    val awsAccessKey = aws.getString("access-key")
    val awsSecretKey = aws.getString("secret-key")

    private val stream = consumer.getConfig("stream")
    val streamName = stream.getString("name")
    val streamDataType = stream.getString("data_type")
  }

  // Initialize
  private implicit val kinesis = createKinesisClient(ConsumerConfig.awsAccessKey, ConsumerConfig.awsSecretKey)
  private var stream: Option[Stream] = None

  // Print all records in the current stream.
  def printRecords() {
    if (stream.isEmpty) {
      stream = Some(Kinesis.stream(ConsumerConfig.streamName))
    }
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
   * Creates a new Kinesis client from provided AWS access key and secret
   * key. If both are set to "cpf", then authenticate using the classpath
   * properties file.
   *
   * @return the initialized AmazonKinesisClient
   */
  private[consumer] def createKinesisClient(
      accessKey: String, secretKey: String): Client =
    if (isCpf(accessKey) && isCpf(secretKey)) {
      Client.fromCredentials(new ClasspathPropertiesFileCredentialsProvider())
    } else if (isCpf(accessKey) || isCpf(secretKey)) {
      throw new RuntimeException("access-key and secret-key must both be set to 'cpf', or neither of them")
    } else {
      Client.fromCredentials(accessKey, secretKey)
    }

  /**
   * Is the access/secret key set to the special value "cpf" i.e. use
   * the classpath properties file for credentials.
   *
   * @param key The key to check
   * @return true if key is cpf, false otherwise
   */
  private[consumer] def isCpf(key: String): Boolean = (key == "cpf")
}
