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

package org.apache.spark.sql.hive.orc

import java.util.{Locale, Properties}
import scala.collection.JavaConversions._
import scala.Some

import org.apache.hadoop.mapred.{JobConf, InputFormat, FileInputFormat, FileSplit}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import org.apache.spark.sql.sources._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{SerializableWritable, Logging}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive.{HiveShim, HadoopTableReader, HiveMetastoreTypes}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.sources.PartitionFiles
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.AttributeReference


/**
 * Allows creation of orc based tables using the syntax
 * `CREATE TEMPORARY TABLE ... USING org.apache.spark.sql.orc`.
 * Currently the only option required is `path`, which should be the location of a collection of,
 * optionally partitioned, orc files.
 */
class DefaultSource
    extends RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for orc tables."))
  }

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    OrcRelation(Seq(checkPath(parameters)), parameters, None)(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    OrcRelation(Seq(checkPath(parameters)), parameters, Some(schema))(sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val path = checkPath(parameters)
    OrcRelation.createEmpty(
      path,
      data.schema.toAttributes,
      false,
      sqlContext.sparkContext.hadoopConfiguration,
      sqlContext)

    val relation = createRelation(sqlContext, parameters, data.schema)
    relation.asInstanceOf[OrcRelation].insert(data, true)
    relation
  }
}

/**
 * Partitioning: Partitions are auto discovered and must be in the form of directories `key=value/`
 * located at `path`.  Currently only a single partitioning column is supported and it must
 * be an integer.  This class supports both fully self-describing data, which contains the partition
 * key, and data where the partition key is only present in the folder structure.  The presence
 * of the partitioning key in the data is also auto-detected.  The `null` partition is not yet
 * supported.
 *
 * Metadata: The metadata is automatically discovered by reading the first orc file present.
 * There is currently no support for working with files that have different schema.  Additionally,
 * when parquet metadata caching is turned on, the FileStatus objects for all data will be cached
 * to improve the speed of interactive querying.  When data is added to a table it must be dropped
 * and recreated to pick up any changes.
 *
 * Statistics: Statistics for the size of the table are automatically populated during metadata
 * discovery.
 */
@DeveloperApi
case class OrcRelation
    (paths: Seq[String], parameters: Map[String, String], maybeSchema: Option[StructType] = None)
    (@transient val sqlContext: SQLContext)
  extends CatalystScan with Logging {

  def sparkContext = sqlContext.sparkContext

  @transient lazy val path =
    parameters.getOrElse("path", sys.error("'path' must be specified for orc tables."))

  var columnsNames: String = _
  var columnsTypes: String = _

  // todo: how to calculate this size
  override val sizeInBytes = ???

  private def getMetaDataReader(origPath: Path, configuration: Option[Configuration]): Reader = {
    val conf = configuration.getOrElse(new Configuration())
    val fs: FileSystem = origPath.getFileSystem(conf)
    OrcFile.createReader(fs, origPath)
  }

  private def orcSchema(
      path: Path,
      conf: Option[Configuration]): StructType = {
    // get the schema info through ORC Reader
    val reader = getMetaDataReader(path, conf)
    require(reader != null, "metadata reader is null!")
    if (reader == null) {
      // return empty seq when saveAsOrcFile
      return StructType(Seq.empty)
    }
    val inspector = reader.getObjectInspector.asInstanceOf[StructObjectInspector]
    // data types that is inspected by this inspector
    val schema = inspector.getTypeName
    // set prop here, initial OrcSerde need it
    val fields = inspector.getAllStructFieldRefs
    val (columns, columnTypes) = fields.map { f =>
      f.getFieldName -> f.getFieldObjectInspector.getTypeName
    }.unzip
    columnsNames = columns.mkString(",")
    columnsTypes = columnTypes.mkString(":")

    HiveMetastoreTypes.toDataType(schema).asInstanceOf[StructType]
  }

  override val schema = maybeSchema.getOrElse(orcSchema(
      path,
      Some(sparkContext.hadoopConfiguration)))

  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    val sc = sparkContext
    val conf: Configuration = sc.hadoopConfiguration

    val fs = FileSystem.get(new java.net.URI(path), conf)

    val setInputPathsFunc: Option[JobConf => Unit] =
       Some((jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path))

    addColumnIds(output, schema.toAttributes, conf)
    val inputClass =
      classOf[OrcInputFormat].asInstanceOf[Class[_ <: InputFormat[NullWritable, Writable]]]

    // use SpecificMutableRow to decrease GC garbage
    val mutableRow = new SpecificMutableRow(output.map(_.dataType))
    val attrsWithIndex = outPut.zipWithIndex
    val confBroadcast = sc.broadcast(new SerializableWritable(conf))

    val rowRdd =
      new HadoopRDD(
        sc,
        confBroadcast,
        setInputPathsFunc,
        inputClass,
        classOf[NullWritable],
        classOf[Writable],
        sc.defaultMinPartitions).mapPartitionsWithInputSplit { (split, iter) =>

        val deserializer = {
          val prop: Properties = new Properties
          prop.setProperty("columns", columnsNames)
          prop.setProperty("columns.types", columnsTypes)

          val serde = new OrcSerde
          serde.initialize(null, prop)
          serde
        }
        HadoopTableReader.fillObject(iter.map(_._2), deserializer, attrsWithIndex, mutableRow)
      }
    rowRdd
  }

  /**
   * add column ids and names
   * @param output
   * @param relationOutput
   * @param conf
   */
  def addColumnIds(output: Seq[Attribute], relationOutput: Seq[Attribute], conf: Configuration) {
    val names = output.map(_.name)
    val fieldIdMap = relationOutput.map(_.name).zipWithIndex.toMap
    val ids = output.map { att =>
      val realName = att.name.toLowerCase(Locale.ENGLISH)
      fieldIdMap.getOrElse(realName, -1)
    }.filter(_ >= 0).map(_.asInstanceOf[Integer])

    assert(ids.size == output.size, "columns id and name length does not match!")
    if (ids != null && !ids.isEmpty) {
      HiveShim.appendReadColumns(conf, ids, names)
    }
  }
}
