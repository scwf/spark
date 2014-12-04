package org.apache.spark.sql.hive.h2

import java.util

import org.apache.spark.sql.catalyst.expressions.{Alias, Min, NamedExpression}
import org.apache.spark.sql.hive.h2.expression.{AliasParser, ExpressionColumnParser}
import org.h2.expression.{Aggregate, ExpressionColumn, Expression}
import org.apache.spark.sql.hive.h2.expression.Alias_H2
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.hive.h2.expression.AggregateParser

/**
 * Created by w00297350 on 2014/12/3.
 */

// parser the select clause
object ExpressionsParser {

  def apply(expressions: util.ArrayList[Expression]):Seq[NamedExpression]=
  {
    val exprArray=new ListBuffer[NamedExpression]

    for(i <- 0 to expressions.size()-1)
    {
      val expr=expressions.get(i);

      expr match
      {
        case expressionColumn:ExpressionColumn =>
          exprArray.append(ExpressionColumnParser(expressionColumn))

        case alias:Alias_H2=>
          exprArray.append(AliasParser(alias).asInstanceOf[NamedExpression])

        case aggregate: Aggregate =>
          exprArray.append(Alias(AggregateParser(aggregate), s"$aggregate@$i")())

        case _ =>
          sys.error("Unsupported the expr parser.")
      }
    }

    return exprArray.toSeq
  }

}
