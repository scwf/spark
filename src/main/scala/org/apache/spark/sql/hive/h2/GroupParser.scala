package org.apache.spark.sql.hive.h2

import java.util

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.h2.expression.ExpressionColumn

import scala.collection.mutable.ListBuffer

/**
 * Created by w00297350 on 2014/12/3.
 */
object GroupParser {

  def apply(expressions: util.ArrayList[org.h2.expression.Expression]):Seq[Expression]=
  {
    val groupSize=expressions.size

    val groupCatalystExpr=new ListBuffer[Expression]

    for(i<- 0 to groupSize -1)
    {
        val expr=expressions.get(i)
        expr match
        {
          case expressionColumn:ExpressionColumn =>
            val columnName=expressionColumn.getColumnName();
            groupCatalystExpr.append(UnresolvedAttribute(columnName))

          case _ =>
            sys.error("unsupport the group expr parser.")
        }
    }

    return groupCatalystExpr.toSeq
  }

}
