package org.apache.spark.sql.hive.h2.expression

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression}
import org.h2.expression.{Aggregate, ExpressionColumn}

/**
 * Created by w00297350 on 2014/12/4.
 */
object AliasParser{

  def apply(alias:Alias_H2) : Expression=
  {
    val aliasName=alias.alias
    val expr=alias.expr;

    expr match
    {
      case expressionColumn:ExpressionColumn =>
        return Alias(ExpressionColumnParser(expressionColumn),aliasName)()

      case aggregate: Aggregate =>
        return Alias(AggregateParser(aggregate),aliasName)()

      case _ =>
        sys.error(s"UnSupported the alias type parser: $aliasName")

    }

    null
  }


}