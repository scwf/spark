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

package org.apache.spark.sql.hive.huawei.execution

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.HiveShim._
import org.apache.spark.sql.hive.{HiveContext, HiveInspectors, HiveShim, SparkHiveWriterContainer, ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.{SerializableWritable, SparkContext, TaskContext}

import scala.collection.JavaConversions._

/**
 * :: DeveloperApi ::
 */
@DeveloperApi
case class WriteToDirectory(
    path: String,
    child: SparkPlan,
    isLocal: Boolean,
    desc: TableDesc) extends UnaryNode with HiveInspectors {

  @transient val hiveContext = sqlContext.asInstanceOf[HiveContext]
  @transient private lazy val context = new Context(hiveContext.hiveconf)
  @transient lazy val outputClass = newSerializer(desc).getSerializedClass

  def output = child.output

  private def newSerializer(tableDesc: TableDesc): Serializer = {
    val serializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(null, tableDesc.getProperties)
    serializer
  }

  // maybe we can make it as a common method, share with `InsertIntoHiveTable`
  def saveAsTempFile(
      sc: SparkContext,
      rdd: RDD[Row],
      valueClass: Class[_],
      fileSinkConf: FileSinkDesc,
      conf: SerializableWritable[JobConf],
      writerContainer: SparkHiveWriterContainer): Unit = {
    assert(valueClass != null, "Output value class not set")
    conf.value.setOutputValueClass(valueClass)

    val outputFileFormatClassName = fileSinkConf.getTableInfo.getOutputFileFormatClassName
    assert(outputFileFormatClassName != null, "Output format class not set")
    conf.value.set("mapred.output.format.class", outputFileFormatClassName)
    conf.value.setOutputCommitter(classOf[FileOutputCommitter])

    FileOutputFormat.setOutputPath(
      conf.value,
      SparkHiveWriterContainer.createPathFromString(fileSinkConf.getDirName, conf.value))
    log.debug("Saving as hadoop file of type " + valueClass.getSimpleName)

    writerContainer.driverSideSetup()
    sc.runJob(rdd, writeToFile _)
    writerContainer.commitJob()

    // Note that this function is executed on executor side
    def writeToFile(context: TaskContext, iterator: Iterator[Row]): Unit = {
      val serializer = newSerializer(fileSinkConf.getTableInfo)
      val standardOI = ObjectInspectorUtils
        .getStandardObjectInspector(
          fileSinkConf.getTableInfo.getDeserializer.getObjectInspector,
          ObjectInspectorCopyOption.JAVA)
        .asInstanceOf[StructObjectInspector]

      val fieldOIs = standardOI.getAllStructFieldRefs.map(_.getFieldObjectInspector).toArray
      val wrappers = fieldOIs.map(wrapperFor)
      val outputData = new Array[Any](fieldOIs.length)

      writerContainer.executorSideSetup(context.stageId, context.partitionId, context.attemptNumber)

      iterator.foreach { row =>
        var i = 0
        while (i < fieldOIs.length) {
          outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row(i))
          i += 1
        }

        writerContainer
          .getLocalFileWriter(row)
          .write(serializer.serialize(outputData, standardOI))
      }

      writerContainer.close()
    }
  }

  protected[sql] lazy val sideEffectResult: Seq[Row] = {
    val jobConf = new JobConf(hiveContext.hiveconf)
    val jobConfSer = new SerializableWritable(jobConf)
    val targetPath = new Path(path)

    val (tmpPath, destPath) = if (isLocal) {
      val localFileSystem = FileSystem.getLocal(jobConf)
      val localPath =  localFileSystem.makeQualified(targetPath)
      // remove old dir
      if (localFileSystem.exists(localPath)) {
        localFileSystem.delete(localPath, true)
      }
      (HiveShim.getExternalTmpPath(context, localPath.toUri), localPath)
    } else {
      val qualifiedPath = FileUtils.makeQualified(targetPath, hiveContext.hiveconf)
      val dfs = qualifiedPath.getFileSystem(jobConf)
      if (dfs.exists(qualifiedPath)) {
        dfs.delete(qualifiedPath, true)
      } else {
        dfs.mkdirs(qualifiedPath.getParent)
      }
      (HiveShim.getExternalTmpPath(context, qualifiedPath.toUri), qualifiedPath)
    }

    val fileSinkConf = new FileSinkDesc(tmpPath.toString, desc, false)
    val isCompressed = hiveContext.hiveconf.getBoolean(
      ConfVars.COMPRESSRESULT.varname, ConfVars.COMPRESSRESULT.defaultBoolVal)

    if (isCompressed) {
      // Please note that isCompressed, "mapred.output.compress", "mapred.output.compression.codec",
      // and "mapred.output.compression.type" have no impact on ORC because it uses table properties
      // to store compression information.
      hiveContext.hiveconf.set("mapred.output.compress", "true")
      fileSinkConf.setCompressed(true)
      fileSinkConf.setCompressCodec(hiveContext.hiveconf.get("mapred.output.compression.codec"))
      fileSinkConf.setCompressType(hiveContext.hiveconf.get("mapred.output.compression.type"))
    }

    val writerContainer = new SparkHiveWriterContainer(jobConf, fileSinkConf)

    saveAsTempFile(hiveContext.sparkContext, child.execute(), outputClass,
      fileSinkConf, jobConfSer, writerContainer)

    val fs = tmpPath.getFileSystem(jobConf)

    // move tmp file to dest dir
    if (isLocal) {
      fs.moveToLocalFile(tmpPath, destPath)
    } else if (!fs.rename(tmpPath, destPath)) {
      throw new IOException("Unable to write data to " + destPath)
    }

    Seq.empty[Row]
  }

  override def executeCollect(): Array[Row] = sideEffectResult.toArray

  override def execute(): RDD[Row] = sqlContext.sparkContext.parallelize(sideEffectResult, 1)
}
