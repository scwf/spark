package org.apache.spark.sql.hive.h2.expression

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.h2.expression.{Comparison, ExpressionColumn, ValueExpression}
import org.h2.message.DbException

/**
 * Created by w00297350 on 2014/12/2.
 */
object ComparisonParser {

  def apply(comparison:Comparison):org.apache.spark.sql.catalyst.expressions.Expression=
  {
    val compareType: Int = comparison.compareType

    val leftExpr: org.h2.expression.Expression = comparison.getExpression(true)
    val rightExpr: org.h2.expression.Expression = comparison.getExpression(false)
    val column: ExpressionColumn = leftExpr.asInstanceOf[ExpressionColumn]
    val columnName: String = column.getColumnName

    val columnExpr: UnresolvedAttribute = new UnresolvedAttribute(columnName)
    var rightCatalystExpr:org.apache.spark.sql.catalyst.expressions.Expression=null;

    //deal the comparison Right Expression Type
    if (rightExpr.isInstanceOf[ValueExpression]) {
      val rightValue: ValueExpression = rightExpr.asInstanceOf[ValueExpression]
      val value = rightValue.getValue(null)
      rightCatalystExpr=ValueParser(value)
    }
    else if (rightExpr.isInstanceOf[ExpressionColumn]) {
      val rightValue: ExpressionColumn = rightExpr.asInstanceOf[ExpressionColumn]
      val rightColumnName=rightValue.getColumnName
      rightCatalystExpr=UnresolvedAttribute(rightColumnName)
    }
    else {
      throw DbException.throwInternalError("not support" )
    }


    compareType match {
      case Comparison.IS_NULL =>

      case Comparison.IS_NOT_NULL =>

      case Comparison.SPATIAL_INTERSECTS =>

      case Comparison.EQUAL =>
        return  EqualTo(columnExpr,rightCatalystExpr)

      case Comparison.EQUAL_NULL_SAFE =>

      case Comparison.BIGGER_EQUAL =>

      case Comparison.BIGGER =>
        return GreaterThan(columnExpr,rightCatalystExpr)

      case Comparison.SMALLER_EQUAL =>

      case Comparison.SMALLER =>
        return LessThan(columnExpr,rightCatalystExpr)

      case Comparison.NOT_EQUAL =>

      case Comparison.NOT_EQUAL_NULL_SAFE =>

      case _ =>

        throw DbException.throwInternalError("compareType=" + compareType)
    }
    sys.error(s"Unsupported h2 sql comparison parser: $compareType")
  }



}
