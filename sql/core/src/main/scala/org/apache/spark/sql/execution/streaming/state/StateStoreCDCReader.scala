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
 * Base class for state store changelog reader
 * @param fm - checkpoint file manager used to manage streaming query checkpoint
 * @param fileToRead - name of file to use to read changelog
 * @param compressionCodec - de-compression method using for reading changelog file
 */
abstract class StateStoreCDCReader(
    fm: CheckpointFileManager,
    // fileToRead: Path,
    stateLocation: Path,
    startVersion: Long,
    endVersion: Long,
    compressionCodec: CompressionCodec,
    keySchema: StructType,
    valueSchema: StructType)
  extends NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)] with Logging {

  class ChangeLogFileIterator(
      stateLocation: Path,
      startVersion: Long,
      endVersion: Long) extends Iterator[Path] {

    // assertions
    assert(true)

    private var currentVersion = startVersion - 1

    def getVersion: Long = currentVersion

    override def hasNext: Boolean = currentVersion < endVersion

    override def next(): Path = {
      currentVersion += 1
      getChangelogPath(stateLocation, currentVersion)
    }
  }


//   private def decompressStream(inputStream: DataInputStream): DataInputStream = {
//     val compressed = compressionCodec.compressedInputStream(inputStream)
//     new DataInputStream(compressed)
//   }

//   private val sourceStream = try {
//     fm.open(fileToRead)
//   } catch {
//     case f: FileNotFoundException =>
//       throw QueryExecutionErrors.failedToReadStreamingStateFileError(fileToRead, f)
//   }
//   protected val input: DataInputStream = decompressStream(sourceStream)


  protected lazy val fileIterator =
    new ChangeLogFileIterator(stateLocation, startVersion, endVersion)

  protected var changelogSuffix: String

  private def getChangelogPath(stateLocation: Path, version: Long): Path =
    new Path(stateLocation, s"$version.$changelogSuffix")

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long)

  def close(): Unit
  // = { if (input != null) input.close() }
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
  extends StateStoreCDCReader(
    fm, stateLocation, startVersion, endVersion, compressionCodec, keySchema, valueSchema) {
  override protected var changelogSuffix: String = "delta"

  private var currentChangelogReader: StateStoreChangelogReader = null

  override def getNext(): (RecordType.Value, UnsafeRow, UnsafeRow, Long) = {
    while (currentChangelogReader == null || !currentChangelogReader.hasNext) {
      if (!fileIterator.hasNext) {
        finished = true
        print("return 1\n")
        return null
      }
      currentChangelogReader =
        new StateStoreChangelogReaderV1(fm, fileIterator.next(), compressionCodec)
    }

    print("return 2\n")
    val readResult = currentChangelogReader.next()
    val keyRow = new UnsafeRow(keySchema.fields.length)
    keyRow.pointTo(readResult._2, readResult._2.length)
    val valueRow = new UnsafeRow(valueSchema.fields.length)
    valueRow.pointTo(readResult._3, readResult._3.length)
    (readResult._1, keyRow, valueRow, fileIterator.getVersion - 1)
  }

//  fix the problem when change if -> while will return null



  override def close(): Unit = {

  }
}
