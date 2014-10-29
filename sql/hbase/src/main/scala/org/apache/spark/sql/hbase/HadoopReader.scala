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

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, SerializableWritable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.KeyValue

/**
 * Helper class for scanning files stored in Hadoop - e.g., to read text file when bulk loading.
 */
private[hbase]
class HadoopReader(
    @transient sc: SparkContext,
    @transient jobConf: JobConf) {

  private val broadcastedHiveConf =
    sc.broadcast(new SerializableWritable(jobConf))

  private val minSplitsPerRDD =
    sc.getConf.get("spark.sql.hbase.minPartitions", sc.defaultMinPartitions.toString).toInt

  private val splitRegex = sc.getConf.get("spark.sql.hbase.splitRegex", ",")

  val inputFormatClass = classOf[TextInputFormat]

  val rowKeyIds = "0,1,2"

  def makeBulkLoadRDD = {
    val rdd = new HadoopRDD(
      sc,
      broadcastedHiveConf.asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      None,
      inputFormatClass,
      classOf[LongWritable],
      classOf[Text],
      minSplitsPerRDD)

    // Todo: use mapPartitions more better, now just simply code
    // Only take the value (skip the key) because Hbase works only with values.
    rdd.map { value =>
    // Todo: needs info which field is rowkey and value from HbaseRelation here
      val fields = value._2.toString.split(splitRegex)
      val rowKey = Bytes.toBytes(fields(0))
      val rowKeyWritable = new SparkImmutableBytesWritable(rowKey)
      val family = Bytes.toBytes("cf")
      val qualifier = Bytes.toBytes("count")
      val hbaseValue = Bytes.toBytes(fields(1))
//      val keyValue = new KeyValue(rowKey, family, qualifier, hbaseValue)
      // we should use Put?, which is better? keyvalue is for one column in column family, right?
      val put = new Put(rowKey)
      put.add(family, qualifier, hbaseValue)
      (rowKeyWritable, put)
    }
  }

}