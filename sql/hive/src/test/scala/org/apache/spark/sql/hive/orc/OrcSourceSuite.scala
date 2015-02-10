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

import org.apache.spark.sql.QueryTest
import org.scalatest.BeforeAndAfterAll
import java.io.File
import org.apache.spark.sql.hive.test.TestHive._

case class OrcData(intField: Int, stringField: String)

abstract class OrcTest extends QueryTest with BeforeAndAfterAll {
  var orcTableDir: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Hack: to prepare orc data files using hive external tables
    orcTableDir = File.createTempFile("orctests", "sparksql")
    orcTableDir.delete()
    orcTableDir.mkdir()

    sparkContext
      .makeRDD(1 to 10)
      .map(i => OrcData(i, s"part-$p"))
      .registerTempTable(s"orc_temp_table")

    sql(s"""
      create external table normal_orc
      (
        intField INT,
        stringField STRING
      )
      STORED AS orc
      location '${orcTableDir.getCanonicalPath}'
    """)

    sql(
      s"""insert into table normal_orc
      select intField, stringField from orc_temp_table""")

  }

  override def afterAll(): Unit = {
    orcTableDir.delete()
  }

  test("select(*)") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM normal_orc_source"),
      10)
  }
}

class OrcSourceSuite extends OrcTest {
  override def beforeAll(): Unit = {
    super.beforeAll()

    sql( s"""
      create temporary table normal_orc_source
      USING org.apache.spark.sql.hive.orc
      OPTIONS (
        path '${new File(partitionedTableDirWithKey, "p=1").getCanonicalPath}'
      )
    """)
  }
}
