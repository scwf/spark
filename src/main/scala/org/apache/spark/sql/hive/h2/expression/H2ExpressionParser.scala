package org.apache.spark.sql.hive.h2.expression

import org.apache.spark.sql.catalyst.expressions.Expression
import org.h2.expression.{ConditionAndOr, Comparison}

/**
 * Created by w00297350 on 2014/12/5.
 */
object H2ExpressionParser {

  def apply(expr_h2:Expression_H2): Expression=
  {
    var expr_catalyst:Expression=null;

    expr_h2 match
    {
      case comparison:Comparison=>
        expr_catalyst=ComparisonParser(comparison)

      case conditionAndOr:ConditionAndOr=>
        expr_catalyst=ConditionAndOrParser(conditionAndOr)

      case _ =>
        sys.error("unsupported the h2 expression parser.")
    }
    expr_catalyst;
  }

}
