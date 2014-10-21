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
package org.apache.spark.sql.hbase

import org.apache.spark.sql.catalyst.{SqlLexical, SqlParser}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._

class HBaseSQLParser extends SqlParser {
  protected val BULK = Keyword("BULK")
  protected val CREATE = Keyword("CREATE")
  protected val DROP = Keyword("DROP")
  protected val ALTER = Keyword("ALTER")
  protected val EXISTS = Keyword("EXISTS")
  protected val MAPPED = Keyword("MAPPED")
  protected val ADD = Keyword("ADD")
  protected val KEYS = Keyword("KEYS")
  protected val COLS = Keyword("COLS")
  protected val BYTE = Keyword("BYTE")
  protected val SHORT = Keyword("SHORT")
  protected val INTEGER = Keyword("INTEGER")
  protected val LONG = Keyword("LONG")
  protected val FLOAT = Keyword("FLOAT")
  protected val DOUBLE = Keyword("DOUBLE")
  protected val BOOLEAN = Keyword("BOOLEAN")

  protected val newReservedWords: Seq[String] =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = new SqlLexical(newReservedWords)

  override protected lazy val query: Parser[LogicalPlan] = (
    select * (
      UNION ~ ALL ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2)} |
        INTERSECT ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2)} |
        EXCEPT ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)} |
        UNION ~ opt(DISTINCT) ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2))}
      )
      | insert | cache | create | drop | alter
    )

  protected lazy val create: Parser[LogicalPlan] =
    CREATE ~> TABLE ~> ident ~
      ("(" ~> tableCols <~ ")") ~
      (MAPPED ~> BY ~> "(" ~> opt(nameSpace)) ~
      (ident <~ ",") ~
      (KEYS ~> "=" ~> "[" ~> keys <~ "]" <~ ",") ~
      (COLS ~> "=" ~> "[" ~> expressions <~ "]" <~ ")") <~ opt(";") ^^ {

      case tableName ~ tableColumns ~ tableNameSpace ~ hbaseTableName ~ keySeq ~ mappingInfo =>
        //Since the lexical can not recognize the symbol "=" as we expected,
        //we compose it to expression first and then translate it into Map[String, (String, String)]
        //TODO: Now get the info by hacking, need to change it into normal way if possible
        val infoMap: Map[String, (String, String)] =
          mappingInfo.map { case EqualTo(e1, e2) =>
            val info = e2.toString.substring(1).split('.')
            if (info.length != 2) throw new Exception("\nSyntx Error of Create Table")
            e1.toString.substring(1) ->(info(0), info(1))
          }.toMap

        val tableColSet = tableColumns.unzip._1.toSet
        val keySet = keySeq.toSet
        if (tableColSet.size != tableColumns.length ||
          keySet.size != keySeq.length ||
          !(keySet union infoMap.keySet).equals(tableColSet)) {
          throw new Exception("\nSyntx Error of Create Table")
        }

        val customizedNameSpace = tableNameSpace.getOrElse("")
        val partitionResultOfTableColumns = tableColumns.partition {
          case (name, _) =>
            keySeq.contains(name)
        }
        val keyColDataTypes = keySeq.toList.map{ orderedKeyCol =>
          partitionResultOfTableColumns._1.find{ allCol =>
            allCol._1 ==  orderedKeyCol
          }.get._2
        }
        val keyColsWithDataTypes = keySeq.zip(keyColDataTypes)
//          zip(partitionResultOfTableColumns._1.map{_._2})
        val nonKeyCols = partitionResultOfTableColumns._2.map {
          case (name, typeOfData) =>
            val infoElem = infoMap.get(name).get
            (name, typeOfData, infoElem._1, infoElem._2)
        }
        CreateHBaseTablePlan(tableName, customizedNameSpace, hbaseTableName,
          keyColsWithDataTypes, nonKeyCols)
    }

  protected lazy val drop: Parser[LogicalPlan] =
    DROP ~> TABLE ~> ident <~ opt(";") ^^ {
      case tableName => DropTablePlan(tableName)
    }

  protected lazy val alter: Parser[LogicalPlan] =
    ALTER ~> TABLE ~> ident ~ DROP ~ ident <~ opt(";") ^^ {
      case tn ~ op ~ col => null
    } | ALTER ~> TABLE ~> ident ~ ADD ~ tableCol ~ (MAPPED ~> BY ~> "(" ~> expressions <~ ")") ^^ {
      case tn ~ op ~ tc ~ cf => null
    }

  protected lazy val tableCol: Parser[(String, String)] =
    ident ~ (STRING | BYTE | SHORT | INTEGER | LONG | FLOAT | DOUBLE | BOOLEAN) ^^ {
      case e1 ~ e2 => (e1, e2)
    }

  protected lazy val nameSpace: Parser[String] = ident <~ "."

  protected lazy val tableCols: Parser[Seq[(String, String)]] = repsep(tableCol, ",")

  protected lazy val keys: Parser[Seq[String]] = repsep(ident, ",")

  protected lazy val expressions: Parser[Seq[Expression]] = repsep(expression, ",")

}

case class CreateHBaseTablePlan(tableName: String,
                                nameSpace: String,
                                hbaseTable: String,
                                keyCols: Seq[(String, String)],
                                nonKeyCols: Seq[(String, String, String, String)]
                                 ) extends Command

case class DropTablePlan(tableName: String) extends Command
