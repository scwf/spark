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

    val leftExpr: Expression_H2 = comparison.getExpression(true)
    val rightExpr: Expression_H2 = comparison.getExpression(false)

    var leftCatalystExpr:Expression=null;
    leftExpr match
    {
      case expressionColumn: ExpressionColumn =>
        leftCatalystExpr=ExpressionColumnParser(expressionColumn)

      case _ =>
        sys.error("unsupported the h2 comparison left type.")
    }

    //deal the comparison Right Expression Type
    var rightCatalystExpr:Expression=null;

    rightExpr match
    {
      case valueExpression:ValueExpression =>
        rightCatalystExpr=ValueParser(valueExpression.value)

      case expressionColumn: ExpressionColumn =>
        leftCatalystExpr=ExpressionColumnParser(expressionColumn)

      case _ =>
        sys.error("unsupported the h2 comparison right type.")
    }


    compareType match {
      case Comparison.IS_NULL =>

      case Comparison.IS_NOT_NULL =>

      case Comparison.SPATIAL_INTERSECTS =>

      case Comparison.EQUAL =>
        return  EqualTo(leftCatalystExpr,rightCatalystExpr)

      case Comparison.EQUAL_NULL_SAFE =>

      case Comparison.BIGGER_EQUAL =>

      case Comparison.BIGGER =>
        return GreaterThan(leftCatalystExpr,rightCatalystExpr)

      case Comparison.SMALLER_EQUAL =>

      case Comparison.SMALLER =>
        return LessThan(leftCatalystExpr,rightCatalystExpr)

      case Comparison.NOT_EQUAL =>

      case Comparison.NOT_EQUAL_NULL_SAFE =>

      case _ =>

        throw DbException.throwInternalError("compareType=" + compareType)
    }
    sys.error(s"Unsupported h2 sql comparison parser: $compareType")
  }



}
