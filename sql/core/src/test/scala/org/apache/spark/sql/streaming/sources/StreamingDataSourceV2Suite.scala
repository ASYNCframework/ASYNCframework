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

package org.apache.spark.sql.streaming.sources

import java.util.Optional

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.streaming.{RateStreamOffset, Sink, StreamingQueryWrapper}
import org.apache.spark.sql.execution.streaming.continuous.ContinuousTrigger
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions, MicroBatchReadSupport, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousReader, MicroBatchReader, Offset, PartitionOffset}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.{OutputMode, StreamTest, Trigger}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

case class FakeReader() extends MicroBatchReader with ContinuousReader {
  def setOffsetRange(start: Optional[Offset], end: Optional[Offset]): Unit = {}
  def getStartOffset: Offset = RateStreamOffset(Map())
  def getEndOffset: Offset = RateStreamOffset(Map())
  def deserializeOffset(json: String): Offset = RateStreamOffset(Map())
  def commit(end: Offset): Unit = {}
  def readSchema(): StructType = StructType(Seq())
  def stop(): Unit = {}
  def mergeOffsets(offsets: Array[PartitionOffset]): Offset = RateStreamOffset(Map())
  def setStartOffset(start: Optional[Offset]): Unit = {}

  def createDataReaderFactories(): java.util.ArrayList[DataReaderFactory[Row]] = {
    throw new IllegalStateException("fake source - cannot actually read")
  }
}

trait FakeMicroBatchReadSupport extends MicroBatchReadSupport {
  override def createMicroBatchReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): MicroBatchReader = FakeReader()
}

trait FakeContinuousReadSupport extends ContinuousReadSupport {
  override def createContinuousReader(
      schema: Optional[StructType],
      checkpointLocation: String,
      options: DataSourceOptions): ContinuousReader = FakeReader()
}

trait FakeStreamWriteSupport extends StreamWriteSupport {
  override def createStreamWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceOptions): StreamWriter = {
    throw new IllegalStateException("fake sink - cannot actually write")
  }
}

class FakeReadMicroBatchOnly extends DataSourceRegister with FakeMicroBatchReadSupport {
  override def shortName(): String = "fake-read-microbatch-only"
}

class FakeReadContinuousOnly extends DataSourceRegister with FakeContinuousReadSupport {
  override def shortName(): String = "fake-read-continuous-only"
}

class FakeReadBothModes extends DataSourceRegister
    with FakeMicroBatchReadSupport with FakeContinuousReadSupport {
  override def shortName(): String = "fake-read-microbatch-continuous"
}

class FakeReadNeitherMode extends DataSourceRegister {
  override def shortName(): String = "fake-read-neither-mode"
}

class FakeWrite extends DataSourceRegister with FakeStreamWriteSupport {
  override def shortName(): String = "fake-write-microbatch-continuous"
}

class FakeNoWrite extends DataSourceRegister {
  override def shortName(): String = "fake-write-neither-mode"
}


case class FakeWriteV1FallbackException() extends Exception

class FakeSink extends Sink {
  override def addBatch(batchId: Long, data: DataFrame): Unit = {}
}

class FakeWriteV1Fallback extends DataSourceRegister
  with FakeStreamWriteSupport with StreamSinkProvider {

  override def createSink(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    partitionColumns: Seq[String],
    outputMode: OutputMode): Sink = {
    new FakeSink()
  }

  override def shortName(): String = "fake-write-v1-fallback"
}


class StreamingDataSourceV2Suite extends StreamTest {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val fakeCheckpoint = Utils.createTempDir()
    spark.conf.set("spark.sql.streaming.checkpointLocation", fakeCheckpoint.getCanonicalPath)
  }

  val readFormats = Seq(
    "fake-read-microbatch-only",
    "fake-read-continuous-only",
    "fake-read-microbatch-continuous",
    "fake-read-neither-mode")
  val writeFormats = Seq(
    "fake-write-microbatch-continuous",
    "fake-write-neither-mode")
  val triggers = Seq(
    Trigger.Once(),
    Trigger.ProcessingTime(1000),
    Trigger.Continuous(1000))

  private def testPositiveCase(readFormat: String, writeFormat: String, trigger: Trigger) = {
    val query = spark.readStream
      .format(readFormat)
      .load()
      .writeStream
      .format(writeFormat)
      .trigger(trigger)
      .start()
    query.stop()
    query
  }

  private def testNegativeCase(
      readFormat: String,
      writeFormat: String,
      trigger: Trigger,
      errorMsg: String) = {
    val ex = intercept[UnsupportedOperationException] {
      testPositiveCase(readFormat, writeFormat, trigger)
    }
    assert(ex.getMessage.contains(errorMsg))
  }

  private def testPostCreationNegativeCase(
      readFormat: String,
      writeFormat: String,
      trigger: Trigger,
      errorMsg: String) = {
    val query = spark.readStream
      .format(readFormat)
      .load()
      .writeStream
      .format(writeFormat)
      .trigger(trigger)
      .start()

    eventually(timeout(streamingTimeout)) {
      assert(query.exception.isDefined)
      assert(query.exception.get.cause != null)
      assert(query.exception.get.cause.getMessage.contains(errorMsg))
    }
  }

  test("disabled v2 write") {
    // Ensure the V2 path works normally and generates a V2 sink..
    val v2Query = testPositiveCase(
      "fake-read-microbatch-continuous", "fake-write-v1-fallback", Trigger.Once())
    assert(v2Query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sink
      .isInstanceOf[FakeWriteV1Fallback])

    // Ensure we create a V1 sink with the config. Note the config is a comma separated
    // list, including other fake entries.
    val fullSinkName = "org.apache.spark.sql.streaming.sources.FakeWriteV1Fallback"
    withSQLConf(SQLConf.DISABLED_V2_STREAMING_WRITERS.key -> s"a,b,c,test,$fullSinkName,d,e") {
      val v1Query = testPositiveCase(
        "fake-read-microbatch-continuous", "fake-write-v1-fallback", Trigger.Once())
      assert(v1Query.asInstanceOf[StreamingQueryWrapper].streamingQuery.sink
        .isInstanceOf[FakeSink])
    }
  }

  // Get a list of (read, write, trigger) tuples for test cases.
  val cases = readFormats.flatMap { read =>
    writeFormats.flatMap { write =>
      triggers.map(t => (write, t))
    }.map {
      case (write, t) => (read, write, t)
    }
  }

  for ((read, write, trigger) <- cases) {
    testQuietly(s"stream with read format $read, write format $write, trigger $trigger") {
      val readSource = DataSource.lookupDataSource(read, spark.sqlContext.conf).newInstance()
      val writeSource = DataSource.lookupDataSource(write, spark.sqlContext.conf).newInstance()
      (readSource, writeSource, trigger) match {
        // Valid microbatch queries.
        case (_: MicroBatchReadSupport, _: StreamWriteSupport, t)
          if !t.isInstanceOf[ContinuousTrigger] =>
          testPositiveCase(read, write, trigger)

        // Valid continuous queries.
        case (_: ContinuousReadSupport, _: StreamWriteSupport, _: ContinuousTrigger) =>
          testPositiveCase(read, write, trigger)

        // Invalid - can't read at all
        case (r, _, _)
            if !r.isInstanceOf[MicroBatchReadSupport]
              && !r.isInstanceOf[ContinuousReadSupport] =>
          testNegativeCase(read, write, trigger,
            s"Data source $read does not support streamed reading")

        // Invalid - can't write
        case (_, w, _) if !w.isInstanceOf[StreamWriteSupport] =>
          testNegativeCase(read, write, trigger,
            s"Data source $write does not support streamed writing")

        // Invalid - trigger is continuous but reader is not
        case (r, _: StreamWriteSupport, _: ContinuousTrigger)
            if !r.isInstanceOf[ContinuousReadSupport] =>
          testNegativeCase(read, write, trigger,
            s"Data source $read does not support continuous processing")

        // Invalid - trigger is microbatch but reader is not
        case (r, _, t)
           if !r.isInstanceOf[MicroBatchReadSupport] && !t.isInstanceOf[ContinuousTrigger] =>
          testPostCreationNegativeCase(read, write, trigger,
            s"Data source $read does not support microbatch processing")
      }
    }
  }
}
