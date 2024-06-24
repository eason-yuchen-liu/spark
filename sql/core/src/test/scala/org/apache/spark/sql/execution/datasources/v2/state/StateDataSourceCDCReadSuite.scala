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

import java.io.File

import org.scalatest.Assertions

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf



class HDFSBackedStateDataSourceReadCDCSuite extends StateDataSourceCDCReadSuite {
  override protected def newStateStoreProvider(): HDFSBackedStateStoreProvider =
    new HDFSBackedStateStoreProvider

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      newStateStoreProvider().getClass.getName)
    // make sure we have a snapshot for every two delta files
    // HDFS maintenance task will not count the latest delta file, which has the same version
    // as the snapshot version
    spark.conf.set(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key, 1)
  }
}

class RocksDBStateDataSourceCDCReadSuite extends StateDataSourceCDCReadSuite {
  override protected def newStateStoreProvider(): RocksDBStateStoreProvider =
    new RocksDBStateStoreProvider

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      newStateStoreProvider().getClass.getName)
    spark.conf.set("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled",
      "false")
  }
}

class RocksDBWithChangelogCheckpointStateDataSourceCDCReaderSuite extends
StateDataSourceCDCReadSuite {
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
    spark.conf.set(SQLConf.STATE_STORE_MIN_DELTAS_FOR_SNAPSHOT.key, 2)
  }
}

abstract class StateDataSourceCDCReadSuite extends StateDataSourceTestBase with Assertions {
  protected def newStateStoreProvider(): StateStoreProvider

  test("cdc read limit state") {
    withTempDir(tempDir => {
      val tempDir2 = new File("/tmp/state/rand")
      import testImplicits._
      spark.conf.set(SQLConf.STREAMING_MAINTENANCE_INTERVAL.key, 500)
      val inputData = MemoryStream[Int]
      val df = inputData.toDF().limit(10)
      testStream(df)(
        StartStream(checkpointLocation = tempDir2.getAbsolutePath),
        AddData(inputData, 1, 2, 3, 4),
        CheckLastBatch(1, 2, 3, 4),
        AddData(inputData, 5, 6, 7, 8),
        CheckLastBatch(5, 6, 7, 8),
        AddData(inputData, 9, 10, 11, 12),
        CheckLastBatch(9, 10)
      )

      val stateDf = spark.read.format("statestore")
        .option(StateSourceOptions.MODE_TYPE, "cdc")
        .option(StateSourceOptions.CDC_START_BATCH_ID, 0)
        .option(StateSourceOptions.CDC_END_BATCH_ID, 2)
        .load(tempDir2.getAbsolutePath)
      stateDf.show()

      val expectedDf = spark.createDataFrame()
    })
  }

  test("cdc read aggregate state") {

  }

  test("cdc read deduplication state") {

  }

  test("cdc read stream-stream join state") {

  }
}
