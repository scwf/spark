package org.apache.spark.sql.hive.h2.expression

import java.util

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, SortOrder}
import org.h2.expression.{Expression, Aggregate, ExpressionColumn}

import scala.collection.mutable.ListBuffer

/**
 * Created by w00297350 on 2014/12/3.
 */
object SortOrderParser {

  def apply(sortOrder: org.h2.result.SortOrder,expressions: util.ArrayList[Expression]):Seq[SortOrder]={
    val queryColumnIndexes=sortOrder.queryColumnIndexes
    val sortTypes=sortOrder.sortTypes

    val queryColumnIndexesSize=queryColumnIndexes.size

    var seqOrderExpr = new ListBuffer[SortOrder]

    for(i <- 0 to queryColumnIndexesSize-1)
    {
      val exprs=expressions.get(queryColumnIndexes.apply(i))
      val sortType=sortTypes.apply(i)

      val sortDirectionExpr= if(sortType==1) Descending else Ascending
      exprs match
      {
        case orderExpressionColumn:ExpressionColumn =>
          val sortOrderExpr=SortOrder(ExpressionColumnParser(orderExpressionColumn), sortDirectionExpr)
          seqOrderExpr.append(sortOrderExpr)

        case alias:Alias_H2 =>
          seqOrderExpr.append(SortOrder(UnresolvedAttribute(alias.alias),sortDirectionExpr))

        case _ =>
          sys.error(s"Unsupported h2 sql parser orderExpression: $exprs")
      }
    }
    seqOrderExpr.toSeq
  }

}
