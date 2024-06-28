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
package org.apache.spark.sql.execution.datasources.v2.state

import org.apache.hadoop.conf.Configuration
import org.scalatest.Assertions

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType




class HDFSBackedStateDataSourceReadCDCSuite extends StateDataSourceChangeDataReadSuite {
  override protected def newStateStoreProvider(): HDFSBackedStateStoreProvider =
    new HDFSBackedStateStoreProvider

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      newStateStoreProvider().getClass.getName)
    // make sure we have a snapshot for every two delta files
    // HDFS maintenance task will not count the latest delta file, which has the same version
    // as the snapshot version
    spark.conf.set(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED, false)
  }
}

class RocksDBWithChangelogCheckpointStateDataSourceCDCReaderSuite extends
StateDataSourceChangeDataReadSuite {
  override protected def newStateStoreProvider(): RocksDBStateStoreProvider =
    new RocksDBStateStoreProvider

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      newStateStoreProvider().getClass.getName)
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
      "true")
    // make sure we have a snapshot for every other checkpoint
    // RocksDB maintenance task will count the latest checkpoint, so we need to set it to 2
    spark.conf.set(SQLConf.STREAMING_NO_DATA_MICRO_BATCHES_ENABLED, false)
  }
}

abstract class StateDataSourceChangeDataReadSuite extends StateDataSourceTestBase with Assertions {

  import testImplicits._
  import StateStoreTestsHelper._

  protected val keySchema: StructType = StateStoreTestsHelper.keySchema
  protected val valueSchema: StructType = StateStoreTestsHelper.valueSchema

  protected def newStateStoreProvider(): StateStoreProvider

  /**
   * Calls the overridable [[newStateStoreProvider]] to create the state store provider instance.
   * Initialize it with the configuration set by child classes.
   *
   * @param checkpointDir        path to store state information
   * @return instance of class extending [[StateStoreProvider]]
   */
  private def getNewStateStoreProvider(checkpointDir: String): StateStoreProvider = {
    val provider = newStateStoreProvider()
    provider.init(
      StateStoreId(checkpointDir, 0, 0),
      keySchema,
      valueSchema,
      NoPrefixKeyStateEncoderSpec(keySchema),
      useColumnFamilies = false,
      StateStoreConf(spark.sessionState.conf),
      new Configuration)
    provider
  }

  test("new getChangeDataReader API of state store provider") {
    def withNewStateStore (provider: StateStoreProvider, version: Int, f: StateStore => Unit):
      Unit = {
      val stateStore = provider.getStore(version)
      f(stateStore)
      stateStore.commit()
    }
    withTempDir { tempDir =>
      val provider = getNewStateStoreProvider(tempDir.getAbsolutePath)
      withNewStateStore(provider, 0, stateStore =>
        put(stateStore, "a", 1, 1)
      )
      withNewStateStore(provider, 1, stateStore =>
        put(stateStore, "b", 2, 2)
      )
      withNewStateStore(provider, 2, stateStore =>
        stateStore.remove(dataToKeyRow("a", 1))
      )
      withNewStateStore(provider, 3, stateStore =>
        stateStore.remove(dataToKeyRow("b", 2))
      )

      val reader =
        provider.asInstanceOf[SupportsFineGrainedReplay].getStateStoreChangeDataReader(1, 4)
//      assert(reader.getNext() === (RecordType.PUT_RECORD, Row()))
//      println(reader.next())
//      println(reader.next())
//      println(reader.next())
//      println(reader.next())
      assert(reader.next() === (RecordType.PUT_RECORD, dataToKeyRow("a", 1), dataToValueRow(1), 0))
      assert(reader.next() === (RecordType.PUT_RECORD, dataToKeyRow("b", 2), dataToValueRow(2), 1))
      assert(reader.next() ===
        (RecordType.DELETE_RECORD, dataToKeyRow("a", 1), null, 2))
      assert(reader.next() ===
        (RecordType.DELETE_RECORD, dataToKeyRow("b", 2), null, 3))
    }

  }

  test("cdc read limit state") {
    withTempDir { tempDir =>
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().limit(10)
      testStream(df)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        AddData(inputData, 1, 2, 3, 4),
        ProcessAllAvailable(),
        AddData(inputData, 5, 6, 7, 8),
        ProcessAllAvailable(),
        AddData(inputData, 9, 10, 11, 12),
        ProcessAllAvailable()
      )

      val stateDf = spark.read.format("statestore")
        .option(StateSourceOptions.MODE_TYPE, "cdc")
        .option(StateSourceOptions.CDC_START_BATCH_ID, 0)
        .option(StateSourceOptions.CDC_END_BATCH_ID, 2)
        .load(tempDir.getAbsolutePath)
      stateDf.show()

      val expectedDf = Seq(
        Row(Row(null), Row(4), "PUT", 0, 0),
        Row(Row(null), Row(8), "PUT", 1, 0),
        Row(Row(null), Row(10), "PUT", 2, 0),
      )

      checkAnswer(stateDf, expectedDf)
    }
  }

  test("cdc read aggregate state") {
    withTempDir { tempDir =>
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().groupBy("value").count()
      testStream(df, OutputMode.Update)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        AddData(inputData, 1, 2, 3, 4),
        ProcessAllAvailable(),
        AddData(inputData, 2, 3, 4, 5),
        ProcessAllAvailable(),
        AddData(inputData, 3, 4, 5, 6),
        ProcessAllAvailable()
      )

      val stateDf = spark.read.format("statestore")
        .option(StateSourceOptions.MODE_TYPE, "cdc")
        .option(StateSourceOptions.CDC_START_BATCH_ID, 0)
        .option(StateSourceOptions.CDC_END_BATCH_ID, 2)
        .load(tempDir.getAbsolutePath)

      val expectedDf = Seq(
        Row(Row(3), Row(1), "PUT", 0, 1),
        Row(Row(3), Row(2), "PUT", 1, 1),
        Row(Row(5), Row(1), "PUT", 1, 1),
        Row(Row(3), Row(3), "PUT", 2, 1),
        Row(Row(5), Row(2), "PUT", 2, 1),
        Row(Row(4), Row(1), "PUT", 0, 2),
        Row(Row(4), Row(2), "PUT", 1, 2),
        Row(Row(4), Row(3), "PUT", 2, 2),
        Row(Row(1), Row(1), "PUT", 0, 3),
        Row(Row(2), Row(1), "PUT", 0, 4),
        Row(Row(2), Row(2), "PUT", 1, 4),
        Row(Row(6), Row(1), "PUT", 2, 4)
      )

      checkAnswer(stateDf, expectedDf)
    }
  }

  test("cdc read deduplication state") {
    withTempDir { tempDir =>
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().dropDuplicates("value")
      testStream(df, OutputMode.Update)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        AddData(inputData, 1, 2, 3, 4),
        ProcessAllAvailable(),
        AddData(inputData, 2, 3, 4, 5),
        ProcessAllAvailable(),
        AddData(inputData, 3, 4, 5, 6),
        ProcessAllAvailable()
      )

      val stateDf = spark.read.format("statestore")
        .option(StateSourceOptions.MODE_TYPE, "cdc")
        .option(StateSourceOptions.CDC_START_BATCH_ID, 0)
        .option(StateSourceOptions.CDC_END_BATCH_ID, 2)
        .load(tempDir.getAbsolutePath)

      val expectedDf = Seq(
        Row(Row(1), Row(null), "PUT", 0, 3),
        Row(Row(2), Row(null), "PUT", 0, 4),
        Row(Row(3), Row(null), "PUT", 0, 1),
        Row(Row(4), Row(null), "PUT", 0, 2),
        Row(Row(5), Row(null), "PUT", 1, 1),
        Row(Row(6), Row(null), "PUT", 2, 4)
      )

      checkAnswer(stateDf, expectedDf)
    }
  }

  test("cdc read stream-stream join state") {
    withTempDir { tempDir =>
      val inputData = MemoryStream[(Int, Long)]
      val leftDf =
        inputData.toDF().select(col("_1").as("leftKey"), col("_2").as("leftValue"))
      val rightDf =
        inputData.toDF().select((col("_1") * 2).as("rightKey"), col("_2").as("rightValue"))
//      val df = getStreamStreamJoinQuery(inputData)
      val df = leftDf.join(rightDf).where("leftKey == rightKey")
      testStream(df)(
        StartStream(checkpointLocation = tempDir.getAbsolutePath),
        AddData(inputData, (1, 1L), (2, 2L)),
        ProcessAllAvailable(),
        AddData(inputData, (3, 3L), (4, 4L)),
        ProcessAllAvailable()
      )

      val keyWithIndexToValueDf = spark.read.format("statestore")
        .option(StateSourceOptions.STORE_NAME, "left-keyWithIndexToValue")
        .option(StateSourceOptions.MODE_TYPE, "cdc")
        .option(StateSourceOptions.CDC_START_BATCH_ID, 0)
        .option(StateSourceOptions.CDC_END_BATCH_ID, 1)
        .load(tempDir.getAbsolutePath)
      keyWithIndexToValueDf.show()

      val keyWithIndexToValueExpectedDf = Seq(
        Row(Row(3, 0), Row(3, 3, false), "PUT", 1, 1),
        Row(Row(4, 0), Row(4, 4, true), "PUT", 1, 2),
        Row(Row(1, 0), Row(1, 1, false), "PUT", 0, 3),
        Row(Row(2, 0), Row(2, 2, false), "PUT", 0, 4),
        Row(Row(2, 0), Row(2, 2, true), "PUT", 0, 4)
      )

      checkAnswer(keyWithIndexToValueDf, keyWithIndexToValueExpectedDf)

      val keyToNumValuesDf = spark.read.format("statestore")
        .option(StateSourceOptions.STORE_NAME, "left-keyToNumValues")
        .option(StateSourceOptions.MODE_TYPE, "cdc")
        .option(StateSourceOptions.CDC_START_BATCH_ID, 0)
        .option(StateSourceOptions.CDC_END_BATCH_ID, 1)
        .load(tempDir.getAbsolutePath)
      keyToNumValuesDf.show()

      val keyToNumValuesDfExpectedDf = Seq(
        Row(Row(3), Row(1), "PUT", 1, 1),
        Row(Row(4), Row(1), "PUT", 1, 2),
        Row(Row(1), Row(1), "PUT", 0, 3),
        Row(Row(2), Row(1), "PUT", 0, 4)
      )

      checkAnswer(keyToNumValuesDf, keyToNumValuesDfExpectedDf)
    }
  }
}
