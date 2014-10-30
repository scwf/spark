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

package org.apache.spark.sql.hbase.execution
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CompatibilitySingletonFactory, HadoopShims}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{ShuffledRDD, RDD}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{LeafNode, UnaryNode, SparkPlan}
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapred.{Reporter, JobConf}
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.hbase._
import org.apache.spark.sql.hbase.HBaseRelation

/**
 * :: DeveloperApi ::
 * The HBase table scan operator.
 */
@DeveloperApi
case class HBaseSQLTableScan(
    relation: HBaseRelation,
    output: Seq[Attribute],
    rowKeyPredicate: Option[Expression],
    valuePredicate: Option[Expression],
    partitionPredicate: Option[Expression],
    coProcessorPlan: Option[SparkPlan])
    (@transient context: HBaseSQLContext)
  extends LeafNode {

  override def execute(): RDD[Row] = {
    new HBaseSQLReaderRDD(
      relation,
      output,
      rowKeyPredicate, // TODO:convert to column pruning preds
      valuePredicate,
      partitionPredicate, // PartitionPred : Option[Expression]
      None, // coprocSubPlan: SparkPlan
      context
    )
  }
}

@DeveloperApi
case class InsertIntoHBaseTable(
    relation: HBaseRelation,
    child: SparkPlan)
    (@transient hbContext: HBaseSQLContext)
  extends UnaryNode {

  override def execute() = {
    val childRdd = child.execute()
    assert(childRdd != null)
    // YZ: to be implemented using sc.runJob() => SparkContext needed here
    childRdd
  }

  override def output = child.output
}

@DeveloperApi
case class BulkLoadIntoTable(path: String, relation: HBaseRelation, isLocal: Boolean)(
  @transient hbContext: HBaseSQLContext) extends LeafNode {

  val jobConf = new JobConf(hbContext.sc.hadoopConfiguration)

  val hadoopReader = new HadoopReader(hbContext.sparkContext, jobConf)

  // atmp path for storing HFile
  val tmpPath = "/bulkload/test"

  override def execute() = {
    val ordering = HBasePartitioner.orderingRowKey
      .asInstanceOf[Ordering[SparkImmutableBytesWritable]]
    val splitKeys = relation.getRegionStartKeys()
    val rdd = hadoopReader.makeBulkLoadRDD
    val partitioner = new HBasePartitioner(rdd)(splitKeys.toArray)
    val shuffled =
      new ShuffledRDD[SparkImmutableBytesWritable, Put, Put](rdd, partitioner).setKeyOrdering(ordering)

    jobConf.setOutputKeyClass(classOf[SparkImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Put])
    jobConf.set("mapred.output.format.class", classOf[HFileOutputFormat].getName)
    jobConf.set("mapred.output.dir", tmpPath)
    shuffled.saveAsHadoopDataset(jobConf)

    null
  }

  def saveAsHFile(
      rdd: RDD[(SparkImmutableBytesWritable, Put)],
      jobConf: SerializableWritable[JobConf],
      path: String) {
    val conf = jobConf.value
    val job = new Job(conf)
    job.setOutputKeyClass(classOf[SparkImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])
    job.setOutputFormatClass(classOf[HFileOutputFormat])
    FileOutputFormat.setOutputPath(job, new Path(path))

    val wrappedConf = new SerializableWritable(job.getConfiguration)
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    val jobtrackerID = formatter.format(new Date())
    val stageId = sqlContext.sparkContext.newRddId()


    def getWriter(
        outFormat: HFileOutputFormat,
        conf: Configuration,
        path: Path,
        reporter: Reporter) = {
      val hadoopShim = CompatibilitySingletonFactory.getInstance(classOf[HadoopShims])
      val context = hadoopShim.createTestTaskAttemptContext(job, "attempt_200707121733_0001_m_000000_0")
      outFormat.getRecordWriter(context)
    }
    ???
  }

  override def output = Nil

}
