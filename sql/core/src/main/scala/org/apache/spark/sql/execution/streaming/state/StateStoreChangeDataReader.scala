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

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.io.CompressionCodec
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.NextIterator

/**
 * Base class representing a iterator that iterates over a range of changelog files in a state
 * store. In each iteration, it will return a tuple of (changeType: [[RecordType]],
 * nested key: [[UnsafeRow]], nested value: [[UnsafeRow]], batchId: [[Long]])
 *
 * @param fm checkpoint file manager used to manage streaming query checkpoint
 * @param stateLocation location of the state store
 * @param startVersion start version of the changelog file to read
 * @param endVersion end version of the changelog file to read
 * @param compressionCodec de-compression method using for reading changelog file
 */
abstract class StateStoreChangeDataReader(
    fm: CheckpointFileManager,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    compressionCodec: CompressionCodec)
  extends NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)] with Logging {

  assert(startVersion >= 1)
  assert(endVersion >= startVersion)

  /**
   * Iterator that iterates over the changelog files in the state store.
   */
  private class ChangeLogFileIterator extends Iterator[Path] {

    private var currentVersion = StateStoreChangeDataReader.this.startVersion - 1

    /** returns the version of the changelog returned by the latest [[next]] function call */
    def getVersion: Long = currentVersion

    override def hasNext: Boolean = currentVersion < StateStoreChangeDataReader.this.endVersion

    override def next(): Path = {
      currentVersion += 1
      getChangelogPath(currentVersion)
    }

    private def getChangelogPath(version: Long): Path =
      new Path(
        StateStoreChangeDataReader.this.stateLocation,
        s"$version.${StateStoreChangeDataReader.this.changelogSuffix}")
  }

  /** file suffix of the changelog files */
  protected var changelogSuffix: String
  private lazy val fileIterator = new ChangeLogFileIterator
  private var changelogReader: StateStoreChangelogReader = null

  /**
   * Get a changelog reader that has at least one record left to read. If there is no readers left,
   * return null.
   */
  protected def currentChangelogReader(): StateStoreChangelogReader = {
    while (changelogReader == null || !changelogReader.hasNext) {
      if (changelogReader != null) {
        changelogReader.close()
      }
      if (!fileIterator.hasNext) {
        finished = true
        return null
      }
      // Todo: Does not support StateStoreChangelogReaderV2
      changelogReader =
        new StateStoreChangelogReaderV1(fm, fileIterator.next(), compressionCodec)
    }
    changelogReader
  }

  /** get the version of the current changelog reader */
  protected def currentChangelogVersion: Long = fileIterator.getVersion

  override def close(): Unit = {
    if (changelogReader != null) {
      changelogReader.close()
    }
  }
}

class HDFSBackedStateStoreCDCReader(
    fm: CheckpointFileManager,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    compressionCodec: CompressionCodec,
    keySchema: StructType,
    valueSchema: StructType)
  extends StateStoreChangeDataReader(
    fm, stateLocation, startVersion, endVersion, compressionCodec) {

  override protected var changelogSuffix: String = "delta"

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    val reader = currentChangelogReader()
    if (reader == null) {
      return null
    }
    val (recordType, keyArray, valueArray, _) = reader.next()
    val keyRow = new UnsafeRow(keySchema.fields.length)
    keyRow.pointTo(keyArray, keyArray.length)
    if (valueArray == null) {
      (recordType, keyRow, null, currentChangelogVersion - 1)
    } else {
      val valueRow = new UnsafeRow(valueSchema.fields.length)
      // If valueSize in existing file is not multiple of 8, floor it to multiple of 8.
      // This is a workaround for the following:
      // Prior to Spark 2.3 mistakenly append 4 bytes to the value row in
      // `RowBasedKeyValueBatch`, which gets persisted into the checkpoint data
      valueRow.pointTo(valueArray, (valueArray.length / 8) * 8)
      (recordType, keyRow, valueRow, currentChangelogVersion - 1)
    }
  }
}

class RocksDBStateStoreCDCReader(
    fm: CheckpointFileManager,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    compressionCodec: CompressionCodec,
    keyValueEncoderMap:
      ConcurrentHashMap[String, (RocksDBKeyStateEncoder, RocksDBValueStateEncoder)])
  extends StateStoreChangeDataReader(
    fm, stateLocation, startVersion, endVersion, compressionCodec) {

  override protected var changelogSuffix: String = "changelog"

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    val reader = currentChangelogReader()
    if (reader == null) {
      return null
    }
    val (recordType, keyArray, valueArray, columnFamily) = reader.next()
    val (rocksDBKeyStateEncoder, rocksDBValueStateEncoder) = keyValueEncoderMap.get(columnFamily)
    val keyRow = rocksDBKeyStateEncoder.decodeKey(keyArray)
    if (valueArray == null) {
      (recordType, keyRow, null, currentChangelogVersion - 1)
    } else {
      val valueRow = rocksDBValueStateEncoder.decodeValue(valueArray)
      (recordType, keyRow, valueRow, currentChangelogVersion - 1)
    }
  }
}
