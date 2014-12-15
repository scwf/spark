package org.apache.spark.sql.hive.h2.expression

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression}
import org.h2.expression.ExpressionColumn

/**
 * Created by w00297350 on 2014/12/3.
 */
object ExpressionColumnParser {

  def apply(expressionColumn:ExpressionColumn): NamedExpression =
  {
    val columnName=expressionColumn.columnName;
    val tableAlias=expressionColumn.tableAlias
    if(tableAlias==null) {
      UnresolvedAttribute(columnName)
    }
    else
    {
      UnresolvedAttribute(tableAlias+"."+columnName)
    }
  }

}


