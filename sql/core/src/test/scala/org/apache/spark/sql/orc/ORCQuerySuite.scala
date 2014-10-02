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

package org.apache.spark.sql.orc

import org.scalatest.{BeforeAndAfterAll, FunSuiteLike}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.util.Utils
import org.apache.spark.sql.{SchemaRDD, TestData, QueryTest}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.catalyst.util.getTempFilePath
import org.apache.spark.sql.test.TestSQLContext._

import java.io.File

case class TestRDDEntry(key: Int, value: String)

case class NullReflectData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    floatField: java.lang.Float,
    doubleField: java.lang.Double,
    booleanField: java.lang.Boolean)

case class OptionalReflectData(
    intField: Option[Int],
    longField: Option[Long],
    floatField: Option[Float],
    doubleField: Option[Double],
    booleanField: Option[Boolean])

case class Nested(i: Int, s: String)

case class Data(array: Seq[Int], nested: Nested)

case class AllDataTypes(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean)

case class AllDataTypesWithNonPrimitiveType(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean,
    array: Seq[Int],
    arrayContainsNull: Seq[Option[Int]],
    map: Map[Int, Long],
    mapValueContainsNull: Map[Int, Option[Long]],
    data: Data)

case class BinaryData(binaryData: Array[Byte])

class OrcQuerySuite extends QueryTest with FunSuiteLike with BeforeAndAfterAll {
  TestData // Load test data tables.

  var testRDD: SchemaRDD = null
  test("Read/Write All Types") {
    val tempDir = getTempFilePath("orcTest").getCanonicalPath
    val range = (0 to 255)
    val data = sparkContext.parallelize(range)
      .map(x => AllDataTypes(s"$x", x, x.toLong, x.toFloat, x.toDouble, x.toShort, x.toByte, x % 2 == 0))

    data.saveAsOrcFile(tempDir)

    checkAnswer(
      orcFile(tempDir),
      data.toSchemaRDD.collect().toSeq)

    Utils.deleteRecursively(new File(tempDir))

  }

  test("Compression options for writing to a Orcfile") {
    val file = getTempFilePath("orcTest")
    val path = file.toString
    val rdd = TestSQLContext.sparkContext.parallelize((1 to 100))
      .map(i => TestRDDEntry(i, s"val_$i"))

    // test default compression codec, now only support zlib
    rdd.saveAsOrcFile(path)
    var actualCodec = OrcFileOperator.readMetaData(new Path(path), Some(new Configuration())).getCompression.name
    assert(actualCodec == "ZLIB")
  }
}
