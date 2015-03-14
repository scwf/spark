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

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.{Row, QueryTest}
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.hive.test.TestHive._

class HuaweiSQLQuerySuite extends QueryTest with BeforeAndAfterAll {
  override def beforeAll() {
    sql("use dialect hiveql")
  }
  override def afterAll() {
    sql("use dialect hql")
  }
  test("test insert overwrite to dir") {
    import java.io.File
    import org.apache.spark.util.Utils

    def checkPath(path: String): Int ={
      val file = new File(path)
      assert(file.exists())
      val length = file.list().length
      // there must be at least two files
      assert(length >= 2)
      length
    }

    val path = Utils.getLocalDir(sparkContext.getConf) + File.pathSeparator + "write_dir"
    checkAnswer(
      sql(s"INSERT OVERWRITE LOCAL DIRECTORY '$path' SELECT * FROM src LIMIT 5"),
      Seq.empty[Row])
    val length1 = checkPath(path)

    checkAnswer(
      sql(s"""INSERT OVERWRITE LOCAL DIRECTORY '$path'
           |ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS orc
           |SELECT * FROM src LIMIT 5""".stripMargin),
      Seq.empty[Row])
    val length2 = checkPath(path)

    assert(length1 == length2)
  }
}
