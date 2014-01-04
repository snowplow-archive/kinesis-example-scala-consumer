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

import java.util.List

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{
  InvalidStateException,
  ShutdownException,
  ThrottlingException
}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{
  IRecordProcessor,
  IRecordProcessorCheckpointer
}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

import scala.util.control.Breaks._
import scala.collection.JavaConversions._

class RecordProcessor extends IRecordProcessor {
  var kinesisShardId: String = _
  var nextCheckpointTimeInMillis: Long = _

  // Backoff and retry settings
  // TODO: Put these in config.
  val BACKOFF_TIME_IN_MILLIS = 3000L
  val NUM_RETRIES = 10

  val CHECKPOINT_INTERVAL_MILLIS = 1000L
    
  @Override
  def initialize(shardId: String) = {
    println("Initializing record processor for shard: " + shardId)
    this.kinesisShardId = shardId
  }

  @Override
  def processRecords(records: List[Record],
      checkpointer: IRecordProcessorCheckpointer) = {
    println(s"Processing ${records.size} records from $kinesisShardId")
    processRecordsWithRetries(records)

    if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
      checkpoint(checkpointer)
      nextCheckpointTimeInMillis =
        System.currentTimeMillis + CHECKPOINT_INTERVAL_MILLIS
    }
  }

  def processRecordsWithRetries(records: List[Record]) = {
    for (record <- records) {
      try {
        println(s"Sequence number: ${record.getSequenceNumber}")
        println(s"Partition key: ${record.getPartitionKey}")
      } catch {
        case t: Throwable =>
          println(s"Caught throwable while processing record $record")
          println(t)
      }
    }
  }

  @Override
  def shutdown(checkpointer: IRecordProcessorCheckpointer,
      reason: ShutdownReason) = {
    println(s"Shutting down record processor for shard: $kinesisShardId")
    if (reason == ShutdownReason.TERMINATE) {
      checkpoint(checkpointer)
    }
  }
    
  def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
    println(s"Checkpointing shard $kinesisShardId")
    var i = 0
    breakable { for (i <- 0 to NUM_RETRIES-1) {
      try {
        checkpointer.checkpoint()
        break
      } catch {
        case se: ShutdownException =>
          println("Caught shutdown exception, skipping checkpoint.", se)
        case e: ThrottlingException =>
          if (i >= (NUM_RETRIES - 1)) {
            println(s"Checkpoint failed after ${i+1} attempts.", e)
          } else {
            println(s"Transient issue when checkpointing - attempt ${i+1} of "
              + NUM_RETRIES, e)
          }
        case e: InvalidStateException =>
          println("Cannot save checkpoint to the DynamoDB table used by " +
            "the Amazon Kinesis Client Library.", e)
      }
      Thread.sleep(BACKOFF_TIME_IN_MILLIS)
    }
  } }
}
