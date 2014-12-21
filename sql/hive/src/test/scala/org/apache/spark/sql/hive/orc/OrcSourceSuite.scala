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

import org.apache.spark.sql.{SQLConf, QueryTest}
import org.apache.spark.sql.hive.test.TestHive
import org.scalatest.BeforeAndAfterAll
import java.io.File
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.catalyst.expressions.Row

// The data where the partitioning key exists only in the directory structure.
case class OrcData(intField: Int, stringField: String)
// The data that also includes the partitioning key
case class OrcDataWithKey(p: Int, intField: Int, stringField: String)

/**
 * A collection of tests for orc data with various forms of partitioning.
 */
abstract class OrcTest extends QueryTest with BeforeAndAfterAll {
  var partitionedTableDir: File = null
  var partitionedTableDirWithKey: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Hack: to prepare orc data files using hive external tables since
    // there is no sink api for external data source now
    partitionedTableDir = File.createTempFile("orctests", "sparksql")
    partitionedTableDir.delete()
    partitionedTableDir.mkdir()

    (1 to 10).foreach { p =>
      sparkContext.makeRDD(1 to 10)
        .map(i => OrcData(i, s"part-$p"))
        .registerTempTable(s"orc_temp_table_$p")
    }

    partitionedTableDirWithKey = File.createTempFile("orctests", "sparksql")
    partitionedTableDirWithKey.delete()
    partitionedTableDirWithKey.mkdir()

    (1 to 10).foreach { p =>
      sparkContext.makeRDD(1 to 10)
        .map(i => OrcDataWithKey(p, i, s"part-$p"))
        .registerTempTable(s"orc_temp_table_key_$p")
    }

    sql(s"""
      create external table partitioned_orc
      (
        intField INT,
        stringField STRING
      )
      PARTITIONED BY (p int)
      STORED AS orc
      location '${partitionedTableDir.getCanonicalPath}'
    """)

    (1 to 10).foreach { p =>
      sql(s"""
      create external table orc_with_key_$p
      (
        p INT,
        intField INT,
        stringField STRING
      )
      STORED AS orc
      location '${new File(partitionedTableDirWithKey.getCanonicalPath, s"p=$p")}'
    """)
    }

    (1 to 10).foreach { p =>
      sql(s"ALTER TABLE partitioned_orc ADD PARTITION (p=$p)")
    }

    (1 to 10).foreach { p =>
      sql(s"""
      insert into table partitioned_orc PARTITION(p=$p)
      select intField, stringField from orc_temp_table_$p
    """)
    }

    (1 to 10).foreach { p =>
      sql(s"""
      insert into table orc_with_key_$p
      select p, intField, stringField from orc_temp_table_key_$p
    """)
    }
  }

  override def afterAll(): Unit = {
    partitionedTableDirWithKey.delete()
    partitionedTableDir.delete()
  }

  Seq("partitioned_orc_source", "partitioned_orc_with_key_source").foreach { table =>
    test(s"project the partitioning column $table") {
      checkAnswer(
        sql(s"SELECT p, count(*) FROM $table group by p order by p"),
        (1, 10) ::
          (2, 10) ::
          (3, 10) ::
          (4, 10) ::
          (5, 10) ::
          (6, 10) ::
          (7, 10) ::
          (8, 10) ::
          (9, 10) ::
          (10, 10) :: Nil
      )
    }

    test(s"project partitioning and non-partitioning columns $table") {
      checkAnswer(
        sql(s"SELECT stringField, p, count(intField) FROM $table GROUP BY p, stringField order by p"),
        ("part-1", 1, 10) ::
          ("part-2", 2, 10) ::
          ("part-3", 3, 10) ::
          ("part-4", 4, 10) ::
          ("part-5", 5, 10) ::
          ("part-6", 6, 10) ::
          ("part-7", 7, 10) ::
          ("part-8", 8, 10) ::
          ("part-9", 9, 10) ::
          ("part-10", 10, 10) :: Nil
      )
    }

    test(s"simple count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table"),
        100)
    }

    test(s"pruned count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p = 1"),
        10)
    }

    test(s"multi-partition pruned count $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE p IN (1,2,3)"),
        30)
    }

    test(s"non-partition predicates $table") {
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $table WHERE intField IN (1,2,3)"),
        30)
    }

    test(s"sum $table") {
      checkAnswer(
        sql(s"SELECT SUM(intField) FROM $table WHERE intField IN (1,2,3) AND p = 1"),
        1 + 2 + 3)
    }

    test(s"hive udfs $table") {
      checkAnswer(
        sql(s"SELECT concat(stringField, stringField) FROM $table"),
        sql(s"SELECT stringField FROM $table").map {
          case Row(s: String) => Row(s + s)
        }.collect().toSeq)
    }
  }

  test("non-part select(*)") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM normal_orc_source"),
      10)
  }
}

class OrcSourceSuite extends OrcTest {
  override def beforeAll(): Unit = {
    super.beforeAll()
    sql( s"""
      create temporary table partitioned_orc_source
      USING org.apache.spark.sql.hive.orc
      OPTIONS (
        path '${partitionedTableDir.getCanonicalPath}'
      )
    """)

    sql( s"""
      create temporary table partitioned_orc_with_key_source
      USING org.apache.spark.sql.hive.orc
      OPTIONS (
        path '${partitionedTableDirWithKey.getCanonicalPath}'
      )
    """)

    sql( s"""
      create temporary table normal_orc_source
      USING org.apache.spark.sql.hive.orc
      OPTIONS (
        path '${new File(partitionedTableDirWithKey, "p=1").getCanonicalPath}'
      )
    """)
  }
}
