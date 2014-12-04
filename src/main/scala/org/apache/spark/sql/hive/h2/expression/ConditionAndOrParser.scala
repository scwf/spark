package org.apache.spark.sql.hive.h2.expression

import org.apache.spark.sql.catalyst.expressions._
import org.h2.expression.{Comparison, ConditionAndOr}

/**
 * Created by w00297350 on 2014/12/2.
 */
object ConditionAndOrParser {

  def apply(conditionAndOr: ConditionAndOr):org.apache.spark.sql.catalyst.expressions.Expression=
  {

    val leftExpression=conditionAndOr.getExpression(true)
    val rightExpression=conditionAndOr.getExpression(false)

    var leftCatalystExpression:org.apache.spark.sql.catalyst.expressions.Expression=null;
    var rightCatalystExpression:org.apache.spark.sql.catalyst.expressions.Expression=null;

    leftExpression match
    {
      case leftConditionAndOr:ConditionAndOr =>
        leftCatalystExpression= ConditionAndOrParser(leftConditionAndOr)

      case leftComparison:Comparison =>
        leftCatalystExpression= ComparisonParser(leftComparison)

      case _ =>

    }

    rightExpression match
    {
      case rightConditionAndOr:ConditionAndOr =>
        rightCatalystExpression= ConditionAndOrParser(rightConditionAndOr)

      case rightComparison:Comparison =>
        rightCatalystExpression= ComparisonParser(rightComparison)

      case _ =>
    }

    conditionAndOr.andOrType match
    {
      case ConditionAndOr.AND =>
        //sql = left.getSQL + "\n    AND " + right.getSQL
        return And(leftCatalystExpression,rightCatalystExpression)

      case ConditionAndOr.OR =>
        //sql = left.getSQL + "\n    OR " + right.getSQL
        return Or(leftCatalystExpression,rightCatalystExpression)

      case _ =>
        sys.error(s"Unsupported h2 sql parser ConditionAndOr: $conditionAndOr.andOrType")

    }
  }

}
