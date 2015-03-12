
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

package org.apache.spark.sql.sql99

import java.util.TimeZone

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{Row, QueryTest}
import org.apache.spark.sql.hive.TestData
import org.apache.spark.sql.hive.test.TestHive

/* Implicits */
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._


class SQLQuerySuite extends QueryTest with BeforeAndAfterAll {

  var origZone: TimeZone = _
  override protected def beforeAll() {
    import org.apache.spark.sql.hive.test.TestHive._

    origZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val testData = TestHive.sparkContext.parallelize(
      (1 to 100).map(i => TestData(i, i.toString))).toDF()
    testData.registerTempTable("testData")
    sql("use dialect sql99")
  }

  override protected def afterAll() {
    TimeZone.setDefault(origZone)
  }

  test("Support top in select") {
    checkAnswer(
      sql(
        "SELECT top 5 key FROM testData"),
      (1 to 5).map(Row(_)).toSeq)

    checkAnswer(
      sql("SELECT top 5 * FROM testData"),
      (1 to 5).map(i => Row(i, i.toString)).toSeq)

    checkAnswer(
      sql("SELECT top 5 key FROM testData where key < 10 order by key desc"),
      (5 to 9).map(Row(_)).reverse.toSeq)

    checkAnswer(
      sql("SELECT top 5 FROM testData where key < 10 order by key desc"),
      (5 to 9).map(i => Row(i, i.toString)).reverse.toSeq)
  }
}
