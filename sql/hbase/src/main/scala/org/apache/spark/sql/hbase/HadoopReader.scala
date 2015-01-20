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

package org.apache.spark.sql.hbase

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.hbase.util.HBaseKVHelper

/**
 * Helper class for scanning files stored in Hadoop - e.g., to read text file when bulk loading.
 */
private[hbase] class HadoopReader(
     @transient sc: SparkContext,
     path: String,
     delimiter: Option[String])(baseRelation: HBaseRelation) {
  /** make RDD[(SparkImmutableBytesWritable, SparkKeyValue)] from text file. */
  private[hbase] def makeBulkLoadRDDFromTextFile = {
    val rdd = sc.textFile(path)
    val splitRegex = delimiter.getOrElse(",")
    val relation = baseRelation

    rdd.mapPartitions { iter =>
      iter.map { line =>
        // we should not reuse the buffer in bulk-loading otherwise it will lead to
        // corrupted as we are reusing same buffer
        val lineBuffer = HBaseKVHelper.createLineBuffer(relation.output)
        // If the last column in the text file is null, the java parser will
        // return a String[] containing only the non-null text values.
        // In this case we need to append another element (null) to
        // the array returned by line.split(splitRegex).
        val keyBytes = new Array[(HBaseRawType, DataType)](relation.keyColumns.size)
        val valueBytes = new Array[HBaseRawType](relation.nonKeyColumns.size)
        var textValueArray = line.split(splitRegex)
        if(textValueArray.length == relation.output.length -1) {
          textValueArray = textValueArray :+ null
        }
        HBaseKVHelper.string2KV(textValueArray, relation, lineBuffer, keyBytes, valueBytes)
        val rowKeyData = HBaseKVHelper.encodingRawKeyColumns(keyBytes)
        (rowKeyData, valueBytes)
      }
    }
  }
}
