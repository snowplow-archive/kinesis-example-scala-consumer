/*
 * Copyright 2012-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.{FileInputStream,IOException}
import java.net.InetAddress
import java.util.{Properties,UUID}

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{
  InitialPositionInStream,
  KinesisClientLibConfiguration,
  Worker
}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory


object KinesisConsumer extends App {
  // TODO: Add these to config.
  val DEFAULT_APP_NAME = "SnowplowExampleConsumer"
  val DEFAULT_STREAM_NAME = "snowplow_thrift"
  val DEFAULT_KINESIS_ENDPOINT = "https://kinesis.us-east-1.amazonaws.com"
    
  // Initial position in the stream when the application starts.
  // LATEST: most recent data.
  // TRIM_HORIZON: oldest available data.
  val DEFAULT_INITIAL_POSITION = InitialPositionInStream.TRIM_HORIZON
    
  val applicationName = DEFAULT_APP_NAME
  val streamName = DEFAULT_STREAM_NAME
  val kinesisEndpoint = DEFAULT_KINESIS_ENDPOINT
  val initialPositionInStream = DEFAULT_INITIAL_POSITION
    
  var kinesisClientLibConfiguration: KinesisClientLibConfiguration = null
    
  // TODO: Replace this with AWS auth. from config.
  configure
  
  println(s"Starting $applicationName")
  println(s"Running $applicationName to process stream $streamName")
  
  val recordProcessorFactory = new RecordProcessorFactory()
  val worker = new Worker(
    recordProcessorFactory,
    kinesisClientLibConfiguration,
    new NullMetricsFactory()
  )

  worker.run()

  // TODO: Replace this with AWS auth. from config.
  def configure = {
    val workerId = InetAddress.getLocalHost().getCanonicalHostName() +
      ":" + UUID.randomUUID()
    println("Using workerId: " + workerId)

    // TODO.
    val credentialsProvider = new ClasspathPropertiesFileCredentialsProvider()
    println("Using credentials with access key id: " +
      credentialsProvider.getCredentials().getAWSAccessKeyId())
    kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
      applicationName,
      streamName, 
      credentialsProvider,
      workerId
    ).withInitialPositionInStream(initialPositionInStream)
  }
}
