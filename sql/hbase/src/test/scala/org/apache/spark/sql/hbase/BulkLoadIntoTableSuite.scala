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

import org.scalatest.FunSuite
import org.apache.spark.{SparkContext, Logging, LocalSparkContext}
import org.apache.spark.rdd.ShuffledRDD
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.util.Bytes

class BulkLoadIntoTableSuite extends FunSuite with LocalSparkContext with Logging{

  test("write data to HFile") {
    sc = new SparkContext("local", "test")
    val jobConf = new JobConf(sc.hadoopConfiguration)
    val hadoopReader = new HadoopReader(sc, jobConf)
    // atmp path for storing HFile
    val tmpPath = "/bulkload/test"
    val ordering = HBasePartitioner.orderingRowKey
      .asInstanceOf[Ordering[SparkImmutableBytesWritable]]
    val splitKeys = (1 to 40).filter(_ % 5 == 0).filter(_ != 40).map { r =>
      new SparkImmutableBytesWritable(Bytes.toBytes(r))
    }
    val rdd = hadoopReader.makeBulkLoadRDD
    val partitioner = new HBasePartitioner(rdd)(splitKeys)
    val shuffled =
      new ShuffledRDD[SparkImmutableBytesWritable, Put, Put](rdd, partitioner).setKeyOrdering(ordering)

    jobConf.setOutputKeyClass(classOf[SparkImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Put])
    jobConf.set("mapred.output.format.class", classOf[HFileOutputFormat].getName)
    jobConf.set("mapred.output.dir", tmpPath)
    shuffled.saveAsHadoopDataset(jobConf)
  }

}
