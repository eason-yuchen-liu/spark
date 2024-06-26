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

package org.apache.spark.sql.execution.streaming.state

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.NextIterator

/**
 * This is an optional trait for [[StateStoreProvider]]s to mix in if they support reading state
 * change data. It is used by the readChangeFeed option of State Data Source.
 */
trait SupportsStateStoreChangeDataFeed {
  def getStateStoreChangeDataReader(startVersion: Long, endVersion: Long):
    StateStoreChangeDataReader
}

/**
 * Base class for state store changelog reader
 * @param fm - checkpoint file manager used to manage streaming query checkpoint
 * @param fileToRead - name of file to use to read changelog
 * @param compressionCodec - de-compression method using for reading changelog file
 */
abstract class StateStoreChangeDataReader(
    fm: CheckpointFileManager,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
  compressionCodec: CompressionCodec)
  extends NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)] with Logging {

  class ChangeLogFileIterator(
      stateLocation: Path,
      startVersion: Long,
      endVersion: Long) extends Iterator[Path] {

    // assertions
    assert(true)

    private var currentVersion = startVersion - 1

    /**
     * returns the version of the return of the latest [[next]] function call
     */
    def getVersion: Long = currentVersion

    override def hasNext: Boolean = currentVersion < endVersion

    override def next(): Path = {
      currentVersion += 1
      getChangelogPath(stateLocation, currentVersion)
    }
  }

  protected lazy val fileIterator =
    new ChangeLogFileIterator(stateLocation, startVersion, endVersion)

  protected var changelogSuffix: String

  private def getChangelogPath(stateLocation: Path, version: Long): Path =
    new Path(stateLocation, s"$version.$changelogSuffix")

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long)

  def close(): Unit
}

class HDFSBackedStateStoreCDCReader(
    fm: CheckpointFileManager,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    compressionCodec: CompressionCodec,
    keySchema: StructType,
    valueSchema: StructType
  )
  extends StateStoreChangeDataReader(
    fm, stateLocation, startVersion, endVersion, compressionCodec) {
  override protected var changelogSuffix: String = "delta"

  private var currentChangelogReader: StateStoreChangelogReader = null

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    while (currentChangelogReader == null || !currentChangelogReader.hasNext) {
      if (currentChangelogReader != null) {
        currentChangelogReader.close()
      }
      if (!fileIterator.hasNext) {
        finished = true
        return null
      }
      currentChangelogReader =
        new StateStoreChangelogReaderV1(fm, fileIterator.next(), compressionCodec)
    }

    val readResult = currentChangelogReader.next()
    val keyRow = new UnsafeRow(keySchema.fields.length)
    keyRow.pointTo(readResult._2, readResult._2.length)
    val valueRow = new UnsafeRow(valueSchema.fields.length)
    // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
    // This is a workaround for the following:
    // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
    // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
    valueRow.pointTo(readResult._3, (readResult._3.length / 8) * 8)
    (readResult._1, keyRow, valueRow, fileIterator.getVersion - 1)
  }

  override def close(): Unit = {
    if (currentChangelogReader != null) {
      currentChangelogReader.close()
    }
  }
}

class RocksDBStateStoreCDCReader(
  fm: CheckpointFileManager,
  stateLocation: Path,
  startVersion: Long,
  endVersion: Long,
  compressionCodec: CompressionCodec,
  keySchema: StructType,
  valueSchema: StructType
)
  extends StateStoreChangeDataReader(
    fm, stateLocation, startVersion, endVersion, compressionCodec) {
  override protected var changelogSuffix: String = "changelog"

  private var currentChangelogReader: StateStoreChangelogReader = null

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    while (currentChangelogReader == null || !currentChangelogReader.hasNext) {
      if (currentChangelogReader != null) {
        currentChangelogReader.close()
        currentChangelogReader = null
      }
      if (!fileIterator.hasNext) {
        finished = true
        return null
      }
      currentChangelogReader =
        new StateStoreChangelogReaderV1(fm, fileIterator.next(), compressionCodec)
    }

    val readResult = currentChangelogReader.next()
    val keyRow = new UnsafeRow(keySchema.fields.length)
    keyRow.pointTo(readResult._2, readResult._2.length)
    val valueRow = new UnsafeRow(valueSchema.fields.length)
    // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
    // This is a workaround for the following:
    // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
    // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
    valueRow.pointTo(readResult._3, (readResult._3.length / 8) * 8)
    (readResult._1, keyRow, valueRow, fileIterator.getVersion - 1)
  }

  override def close(): Unit = {
    if (currentChangelogReader != null) {
      currentChangelogReader.close()
    }
  }
}
