package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.expressions.{Alias, NamedExpression, Expression}

/**
 * Created by w00297350 on 2014/12/16.
 */
package object h2 {
  //copy from catalyst Sql Parser
   def assignAliases(exprs: Seq[Expression]): Seq[NamedExpression] = {
    exprs.zipWithIndex.map {
      case (ne: NamedExpression, _) => ne
      case (e, i) => Alias(e, s"c$i")()
    }
  }
}
