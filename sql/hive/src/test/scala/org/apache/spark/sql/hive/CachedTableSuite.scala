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

package org.apache.spark.sql.hive

import org.apache.spark.sql.columnar.{InMemoryColumnarTableScan, InMemoryRelation}
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.{Row, QueryTest, SchemaRDD}
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.rdd.RDD
import java.util.UUID

class CachedTableSuite extends QueryTest {
  /**
   * Throws a test failed exception when the number of cached tables differs from the expected
   * number.
   */
  def assertCached(query: SchemaRDD, numCachedTables: Int = 1): Unit = {
    val planWithCaching = query.queryExecution.withCachedData
    val cachedData = planWithCaching collect {
      case cached: InMemoryRelation => cached
    }

    assert(
      cachedData.size == numCachedTables,
      s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
        planWithCaching)
  }

  def rddIdOf(tableName: String): Int = {
    val executedPlan = table(tableName).queryExecution.executedPlan
    executedPlan.collect {
      case InMemoryColumnarTableScan(_, _, relation) =>
        relation.cachedColumnBuffers.id
      case _ =>
        fail(s"Table $tableName is not cached\n" + executedPlan)
    }.head
  }

  def isMaterialized(rddId: Int): Boolean = {
    sparkContext.env.blockManager.get(RDDBlockId(rddId, 0)).nonEmpty
  }

  test("cache table") {
    val preCacheResults = sql("SELECT * FROM src").collect().toSeq

    cacheTable("src")
    assertCached(sql("SELECT * FROM src"))

    checkAnswer(
      sql("SELECT * FROM src"),
      preCacheResults)

    uncacheTable("src")
    assertCached(sql("SELECT * FROM src"), 0)
  }

  test("wfwfwf") {

    def getRDD(sqlText: String): RDD[Row] = {
      val name: String = UUID.randomUUID.toString
      val rdd: SchemaRDD = sql(sqlText)
      rdd.registerTempTable(name)
      cacheTable(name)
      rdd
    }

    val rdd1 = getRDD("select f1,f2,f3,lable from admission")
    //  println("rdd 1 count : " + rdd1.count())

    val zippedRDD1 = rdd1.zipWithIndex()
    println("zipped count : " + zippedRDD1.count())

    val mapedRDD1 = zippedRDD1.map(_.swap)

    val rdd2 = getRDD("select f1 as f11,f2 as f22,f3 as f33,lable as labelx from admission")
      .zipWithIndex().map(_.swap)

    println("rdd 2 count : " + rdd2.count())
    val rdd3 = mapedRDD1.join(rdd2)

    rdd3.foreach(println)
  }

  test("cache invalidation") {
    sql("CREATE TABLE cachedTable(key INT, value STRING)")

    sql("INSERT INTO TABLE cachedTable SELECT * FROM src")
    checkAnswer(sql("SELECT * FROM cachedTable"), table("src").collect().toSeq)

    cacheTable("cachedTable")
    checkAnswer(sql("SELECT * FROM cachedTable"), table("src").collect().toSeq)

    sql("INSERT INTO TABLE cachedTable SELECT * FROM src")
    checkAnswer(
      sql("SELECT * FROM cachedTable"),
      table("src").collect().toSeq ++ table("src").collect().toSeq)

    sql("DROP TABLE cachedTable")
  }

  test("Drop cached table") {
    sql("CREATE TABLE test(a INT)")
    cacheTable("test")
    sql("SELECT * FROM test").collect()
    sql("DROP TABLE test")
    intercept[org.apache.hadoop.hive.ql.metadata.InvalidTableException] {
      sql("SELECT * FROM test").collect()
    }
  }

  test("DROP nonexistant table") {
    sql("DROP TABLE IF EXISTS nonexistantTable")
  }

  test("correct error on uncache of non-cached table") {
    intercept[IllegalArgumentException] {
      TestHive.uncacheTable("src")
    }
  }

  test("'CACHE TABLE' and 'UNCACHE TABLE' HiveQL statement") {
    TestHive.sql("CACHE TABLE src")
    assertCached(table("src"))
    assert(TestHive.isCached("src"), "Table 'src' should be cached")

    TestHive.sql("UNCACHE TABLE src")
    assertCached(table("src"), 0)
    assert(!TestHive.isCached("src"), "Table 'src' should not be cached")
  }

  test("CACHE TABLE tableName AS SELECT * FROM anotherTable") {
    sql("CACHE TABLE testCacheTable AS SELECT * FROM src")
    assertCached(table("testCacheTable"))

    val rddId = rddIdOf("testCacheTable")
    assert(
      isMaterialized(rddId),
      "Eagerly cached in-memory table should have already been materialized")

    uncacheTable("testCacheTable")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE TABLE tableName AS SELECT ...") {
    sql("CACHE TABLE testCacheTable AS SELECT key FROM src LIMIT 10")
    assertCached(table("testCacheTable"))

    val rddId = rddIdOf("testCacheTable")
    assert(
      isMaterialized(rddId),
      "Eagerly cached in-memory table should have already been materialized")

    uncacheTable("testCacheTable")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE LAZY TABLE tableName") {
    sql("CACHE LAZY TABLE src")
    assertCached(table("src"))

    val rddId = rddIdOf("src")
    assert(
      !isMaterialized(rddId),
      "Lazily cached in-memory table shouldn't be materialized eagerly")

    sql("SELECT COUNT(*) FROM src").collect()
    assert(
      isMaterialized(rddId),
      "Lazily cached in-memory table should have been materialized")

    uncacheTable("src")
    assert(!isMaterialized(rddId), "Uncached in-memory table should have been unpersisted")
  }

  test("CACHE TABLE with Hive UDF") {
    sql("CACHE TABLE udfTest AS SELECT * FROM src WHERE floor(key) = 1")
    assertCached(table("udfTest"))
    uncacheTable("udfTest")
  }
}
