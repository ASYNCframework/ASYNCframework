/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming.continuous

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.v2.{DataSourceRDDPartition, RowToUnsafeDataReader}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousDataReader, PartitionOffset}
import org.apache.spark.util.ThreadUtils

class ContinuousDataSourceRDD(
    sc: SparkContext,
    sqlContext: SQLContext,
    @transient private val readerFactories: java.util.List[DataReaderFactory[UnsafeRow]])
  extends RDD[UnsafeRow](sc, Nil) {

  private val dataQueueSize = sqlContext.conf.continuousStreamingExecutorQueueSize
  private val epochPollIntervalMs = sqlContext.conf.continuousStreamingExecutorPollIntervalMs

  override protected def getPartitions: Array[Partition] = {
    readerFactories.asScala.zipWithIndex.map {
      case (readerFactory, index) => new DataSourceRDDPartition(index, readerFactory)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    // If attempt number isn't 0, this is a task retry, which we don't support.
    if (context.attemptNumber() != 0) {
      throw new ContinuousTaskRetryException()
    }

    val reader = split.asInstanceOf[DataSourceRDDPartition[UnsafeRow]]
      .readerFactory.createDataReader()

    val coordinatorId = context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)

    // This queue contains two types of messages:
    // * (null, null) representing an epoch boundary.
    // * (row, off) containing a data row and its corresponding PartitionOffset.
    val queue = new ArrayBlockingQueue[(UnsafeRow, PartitionOffset)](dataQueueSize)

    val epochPollFailed = new AtomicBoolean(false)
    val epochPollExecutor = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
      s"epoch-poll--$coordinatorId--${context.partitionId()}")
    val epochPollRunnable = new EpochPollRunnable(queue, context, epochPollFailed)
    epochPollExecutor.scheduleWithFixedDelay(
      epochPollRunnable, 0, epochPollIntervalMs, TimeUnit.MILLISECONDS)

    // Important sequencing - we must get start offset before the data reader thread begins
    val startOffset = ContinuousDataSourceRDD.getBaseReader(reader).getOffset

    val dataReaderFailed = new AtomicBoolean(false)
    val dataReaderThread = new DataReaderThread(reader, queue, context, dataReaderFailed)
    dataReaderThread.setDaemon(true)
    dataReaderThread.start()

    context.addTaskCompletionListener(_ => {
      dataReaderThread.interrupt()
      epochPollExecutor.shutdown()
    })

    val epochEndpoint = EpochCoordinatorRef.get(coordinatorId, SparkEnv.get)
    new Iterator[UnsafeRow] {
      private val POLL_TIMEOUT_MS = 1000

      private var currentEntry: (UnsafeRow, PartitionOffset) = _
      private var currentOffset: PartitionOffset = startOffset
      private var currentEpoch =
        context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

      override def hasNext(): Boolean = {
        while (currentEntry == null) {
          if (context.isInterrupted() || context.isCompleted()) {
            currentEntry = (null, null)
          }
          if (dataReaderFailed.get()) {
            throw new SparkException("data read failed", dataReaderThread.failureReason)
          }
          if (epochPollFailed.get()) {
            throw new SparkException("epoch poll failed", epochPollRunnable.failureReason)
          }
          currentEntry = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        }

        currentEntry match {
          // epoch boundary marker
          case (null, null) =>
            epochEndpoint.send(ReportPartitionOffset(
              context.partitionId(),
              currentEpoch,
              currentOffset))
            currentEpoch += 1
            currentEntry = null
            false
          // real row
          case (_, offset) =>
            currentOffset = offset
            true
        }
      }

      override def next(): UnsafeRow = {
        if (currentEntry == null) throw new NoSuchElementException("No current row was set")
        val r = currentEntry._1
        currentEntry = null
        r
      }
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition[UnsafeRow]].readerFactory.preferredLocations()
  }
}

case class EpochPackedPartitionOffset(epoch: Long) extends PartitionOffset

class EpochPollRunnable(
    queue: BlockingQueue[(UnsafeRow, PartitionOffset)],
    context: TaskContext,
    failedFlag: AtomicBoolean)
  extends Thread with Logging {
  private[continuous] var failureReason: Throwable = _

  private val epochEndpoint = EpochCoordinatorRef.get(
    context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY), SparkEnv.get)
  private var currentEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

  override def run(): Unit = {
    try {
      val newEpoch = epochEndpoint.askSync[Long](GetCurrentEpoch)
      for (i <- currentEpoch to newEpoch - 1) {
        queue.put((null, null))
        logDebug(s"Sent marker to start epoch ${i + 1}")
      }
      currentEpoch = newEpoch
    } catch {
      case t: Throwable =>
        failureReason = t
        failedFlag.set(true)
        throw t
    }
  }
}

class DataReaderThread(
    reader: DataReader[UnsafeRow],
    queue: BlockingQueue[(UnsafeRow, PartitionOffset)],
    context: TaskContext,
    failedFlag: AtomicBoolean)
  extends Thread(
    s"continuous-reader--${context.partitionId()}--" +
    s"${context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY)}") {
  private[continuous] var failureReason: Throwable = _

  override def run(): Unit = {
    TaskContext.setTaskContext(context)
    val baseReader = ContinuousDataSourceRDD.getBaseReader(reader)
    try {
      while (!context.isInterrupted && !context.isCompleted()) {
        if (!reader.next()) {
          // Check again, since reader.next() might have blocked through an incoming interrupt.
          if (!context.isInterrupted && !context.isCompleted()) {
            throw new IllegalStateException(
              "Continuous reader reported no elements! Reader should have blocked waiting.")
          } else {
            return
          }
        }

        queue.put((reader.get().copy(), baseReader.getOffset))
      }
    } catch {
      case _: InterruptedException if context.isInterrupted() =>
        // Continuous shutdown always involves an interrupt; do nothing and shut down quietly.

      case t: Throwable =>
        failureReason = t
        failedFlag.set(true)
        // Don't rethrow the exception in this thread. It's not needed, and the default Spark
        // exception handler will kill the executor.
    } finally {
      reader.close()
    }
  }
}

object ContinuousDataSourceRDD {
  private[continuous] def getBaseReader(reader: DataReader[UnsafeRow]): ContinuousDataReader[_] = {
    reader match {
      case r: ContinuousDataReader[UnsafeRow] => r
      case wrapped: RowToUnsafeDataReader =>
        wrapped.rowReader.asInstanceOf[ContinuousDataReader[Row]]
      case _ =>
        throw new IllegalStateException(s"Unknown continuous reader type ${reader.getClass}")
    }
  }
}
