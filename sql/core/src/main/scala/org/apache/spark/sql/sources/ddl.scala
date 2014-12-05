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

package org.apache.spark.sql.sources

import org.apache.spark.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.util.Utils

import scala.language.implicitConversions
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.PackratParsers

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{SqlParser, SqlLexical}

/**
 * A parser for foreign DDL commands.
 */
private[sql] class DDLParser extends SqlParser with Logging {

  def apply(input: String): Option[LogicalPlan] = {
    phrase(ddl)(new lexical.Scanner(input)) match {
      case Success(r, x) => Some(r)
      case x =>
        logDebug(s"Not recognized as DDL: $x")
        None
    }
  }

  protected implicit def asParser(k: Keyword): Parser[String] =
    lexical.allCaseVersions(k.str).map(x => x : Parser[String]).reduce(_ | _)

  protected val CREATE = Keyword("CREATE")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val TABLE = Keyword("TABLE")
  protected val USING = Keyword("USING")
  protected val OPTIONS = Keyword("OPTIONS")
  protected val LOAD = Keyword("LOAD")
  protected val DATA = Keyword("DATA")
  protected val LOCAL = Keyword("LOCAL")
  protected val INPATH = Keyword("INPATH")
  protected val OVERWRITE = Keyword("OVERWRITE")
  protected val TERMINATED = Keyword("TERMINATED")
  protected val FIELDS = Keyword("FIELDS")
  protected val INTO = Keyword("INTO")

  protected lazy val start: Parser[LogicalPlan] =
    ( select *
      ( UNION ~ ALL        ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2) }
      | INTERSECT          ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2) }
      | EXCEPT             ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)}
      | UNION ~ DISTINCT.? ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2)) }
      )
    | insert
    | createTable
    | load
    )

  // Use reflection to find the reserved words defined in this class.
  protected val reservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(reservedWords)

  protected lazy val ddl: Parser[LogicalPlan] = createTable

  /**
   * CREATE TEMPORARY TABLE avroTable
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")
   */
  protected lazy val createTable: Parser[LogicalPlan] =
    CREATE ~ TEMPORARY ~ TABLE ~> ident ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case tableName ~ provider ~ opts =>
        CreateTableUsing(tableName, provider, opts)
    }

  // Load syntax:
  // LOAD DATA [LOCAL] INPATH filepath [OVERWRITE] INTO TABLE tablename [FIELDS TERMINATED BY char]
  protected lazy val load: Parser[LogicalPlan] =
    (
      (LOAD ~> DATA ~> INPATH ~> stringLit) ~
        (opt(OVERWRITE) ~> INTO ~> TABLE ~> relation ) ~
        (FIELDS ~> TERMINATED ~> BY ~> stringLit) ^^ {
        case filePath ~ table ~ delimiter => BulkLoadPlan(filePath, table, false, delimiter)
      }
        | (LOAD ~> DATA ~> LOCAL ~> INPATH ~> stringLit) ~
        (opt(OVERWRITE) ~> INTO ~> TABLE ~> relation) ~
        (FIELDS ~> TERMINATED ~> BY ~> stringLit) ^^ {
        case filePath ~ table ~ delimiter => BulkLoadPlan(filePath, table, true, delimiter)
      }
      )

  protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

  protected lazy val pair: Parser[(String, String)] = ident ~ stringLit ^^ { case k ~ v => (k,v) }
}

private[sql] case class CreateTableUsing(
    tableName: String,
    provider: String,
    options: Map[String, String]) extends RunnableCommand {

  def run(sqlContext: SQLContext) = {
    val loader = Utils.getContextOrSparkClassLoader
    val clazz: Class[_] = try loader.loadClass(provider) catch {
      case cnf: java.lang.ClassNotFoundException =>
        try loader.loadClass(provider + ".DefaultSource") catch {
          case cnf: java.lang.ClassNotFoundException =>
            sys.error(s"Failed to load class for data source: $provider")
        }
    }
    val dataSource = clazz.newInstance().asInstanceOf[org.apache.spark.sql.sources.RelationProvider]
    val relation = dataSource.createRelation(sqlContext, options)

    sqlContext.baseRelationToSchemaRDD(relation).registerTempTable(tableName)
    Seq.empty
  }
}
