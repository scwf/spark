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

import org.apache.hadoop.mapred.{JobConf, InputFormat, FileInputFormat, FileSplit}
import org.apache.hadoop.io.{NullWritable, Writable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.hive.ql.io.orc._
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import org.apache.spark.sql.sources.{PartitionFiles, CatalystScan, BaseRelation, RelationProvider}
import org.apache.spark.sql.SQLContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{SerializableWritable, Logging}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.hive.{HiveShim, HadoopTableReader, HiveMetastoreTypes}
import org.apache.spark.rdd.{HadoopRDD, RDD}


/**
 * Allows creation of orc based tables using the syntax
 * `CREATE TEMPORARY TABLE ... USING org.apache.spark.sql.orc`.
 * Currently the only option required is `path`, which should be the location of a collection of,
 * optionally partitioned, orc files.
 */
class DefaultSource extends RelationProvider {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val path =
      parameters.getOrElse("path", sys.error("'path' must be specified for orc tables."))

    OrcRelation(path)(sqlContext)
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
case class OrcRelation(path: String)(@transient val sqlContext: SQLContext)
  extends CatalystScan with Logging {

  def sparkContext = sqlContext.sparkContext

  var columnsNames: String = _
  var columnsTypes: String = _

  // Minor Hack: scala does not seem to respect @transient for vals declared via extraction
  @transient
  private var partitionKeys: Seq[String] = _
  @transient
  private var partitions: Seq[PartitionFiles] = _
  discoverPartitions()

  // TODO: Only finds the first partition, assumes the key is of type Integer...
  private def discoverPartitions() = {
    val fs = FileSystem.get(new java.net.URI(path), sparkContext.hadoopConfiguration)
    val partValue = "([^=]+)=([^=]+)".r

    val childrenOfPath = fs.listStatus(new Path(path)).filterNot(_.getPath.getName.startsWith("_"))
    val childDirs = childrenOfPath.filter(s => s.isDir)

    if (childDirs.size > 0) {
      val partitionPairs = childDirs.map(_.getPath.getName).map {
        case partValue(key, value) => (key, value)
      }

      val foundKeys = partitionPairs.map(_._1).distinct
      if (foundKeys.size > 1) {
        sys.error(s"Too many distinct partition keys: $foundKeys")
      }

      // Do a parallel lookup of partition metadata.
      val partitionFiles =
        childDirs.par.map { d =>
          fs.listStatus(d.getPath)
            // TODO: Is there a standard hadoop function for this?
            .filterNot(_.getPath.getName.startsWith("_"))
            .filterNot(_.getPath.getName.startsWith("."))
        }.seq

      partitionKeys = foundKeys.toSeq
      partitions = partitionFiles.zip(partitionPairs).map { case (files, (key, value)) =>
        PartitionFiles(Map(key -> value.toInt), files)
      }.toSeq
    } else {
      partitionKeys = Nil
      partitions = PartitionFiles(Map.empty, childrenOfPath) :: Nil
    }
  }


  override val sizeInBytes = partitions.flatMap(_.files).map(_.getLen).sum

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

  val dataSchema = orcSchema(
      partitions.head.files.head.getPath,
      Some(sparkContext.hadoopConfiguration))

  val dataIncludesKey =
    partitionKeys.headOption.map(dataSchema.fieldNames.contains(_)).getOrElse(true)

  override val schema =
    if (dataIncludesKey) {
      dataSchema
    } else {
      StructType(dataSchema.fields :+ StructField(partitionKeys.head, IntegerType))
    }

  override def buildScan(output: Seq[Attribute], predicates: Seq[Expression]): RDD[Row] = {
    val sc = sparkContext
    val conf: Configuration = sc.hadoopConfiguration
    val partitionKeySet = partitionKeys.toSet
    val rawPredicate =
      predicates
        .filter(_.references.map(_.name).toSet.subsetOf(partitionKeySet))
        .reduceOption(And)
        .getOrElse(Literal(true))

    // Translate the predicate so that it reads from the information derived from the
    // folder structure
    val castedPredicate = rawPredicate transform {
      case a: AttributeReference =>
        val idx = partitionKeys.indexWhere(a.name == _)
        BoundReference(idx, IntegerType, nullable = true)
    }

    val inputData = new GenericMutableRow(partitionKeys.size)
    val pruningCondition = InterpretedPredicate(castedPredicate)

    val selectedPartitions =
      if (partitionKeys.nonEmpty && predicates.nonEmpty) {
        partitions.filter { part =>
          inputData(0) = part.partitionValues.values.head
          pruningCondition(inputData)
        }
      } else {
        partitions
      }

    val fs = FileSystem.get(new java.net.URI(path), conf)
    val selectedFiles = selectedPartitions.flatMap(_.files).map(f => fs.makeQualified(f.getPath))

    val setInputPathsFunc: Option[JobConf => Unit] = if (selectedFiles.nonEmpty) {
       Some((jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path))
    } else {
      None
    }

    val addOutPut = if (!dataIncludesKey) {
      output.filter(att => !partitionKeys.contains(att.name))
    } else {
      output
    }

    // The ordinal for the partition key in the result row, if requested.
    val partitionKeyLocation =
      partitionKeys
        .headOption
        .map(k => output.map(_.name).indexOf(k))


    addColumnIds(addOutPut, schema.toAttributes, conf)
    val inputClass =
      classOf[OrcInputFormat].asInstanceOf[Class[_ <: InputFormat[NullWritable, Writable]]]

    // use SpecificMutableRow to decrease GC garbage
    val mutableRow = new SpecificMutableRow(output.map(_.dataType))
    val attrsWithIndex = addOutPut.zipWithIndex
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
        val partValue = "([^=]+)=([^=]+)".r
        val partValues =
          split.asInstanceOf[FileSplit]
            .getPath
            .toString
            .split("/")
            .flatMap {
            case partValue(key, value) => Some(key -> value)
            case _ => None
          }.toMap

        val deserializer = {
          val prop: Properties = new Properties
          prop.setProperty("columns", columnsNames)
          prop.setProperty("columns.types", columnsTypes)

          val serde = new OrcSerde
          serde.initialize(null, prop)
          serde
        }
        if (partitionKeyLocation.isEmpty || partitionKeyLocation.get == -1) {
          HadoopTableReader.fillObject(iter.map(_._2), deserializer, attrsWithIndex, mutableRow)
        } else {
          val currentValue = partValues.values.head.toInt
          HadoopTableReader.fillObject(
            iter.map(_._2),
            deserializer,
            attrsWithIndex,
            mutableRow,
            partitionKeyLocation,
            currentValue)
        }
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
