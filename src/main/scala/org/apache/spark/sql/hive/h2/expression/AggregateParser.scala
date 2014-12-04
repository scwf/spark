package org.apache.spark.sql.hive.h2.expression

import org.apache.spark.sql.catalyst.expressions.{Min, Alias, Expression}
import org.h2.expression.{ExpressionColumn, Aggregate}

/**
 * Created by w00297350 on 2014/12/4.
 */
object AggregateParser {

  def apply(aggregate: Aggregate):Expression=
  {
    aggregate.`type` match {
      case Aggregate.GROUP_CONCAT =>
      //return getSQLGroupConcat

      case Aggregate.COUNT_ALL =>
      //return "COUNT(*)"

      case Aggregate.COUNT =>
      //text = "COUNT"
      //break //todo: break is not supported

      case Aggregate.SELECTIVITY =>
      //ext = "SELECTIVITY"
      // break //todo: break is not supported

      case Aggregate.HISTOGRAM =>
      //text = "HISTOGRAM"
      //break //todo: break is not supported

      case Aggregate.SUM =>


      case Aggregate.MIN =>
        aggregate.on match
        {
          case expressionColumn:ExpressionColumn =>
            return Min(ExpressionColumnParser(expressionColumn))

          case _ =>

        }
      //text = "MIN"
      //break //todo: break is not supported

      case Aggregate.MAX =>
      //text = "MAX"
      //break //todo: break is not supported

      case Aggregate.AVG =>
      //text = "AVG"
      //break //todo: break is not supported

      case Aggregate.STDDEV_POP =>
      //text = "STDDEV_POP"
      //break //todo: break is not supported

      case Aggregate.STDDEV_SAMP =>
      //text = "STDDEV_SAMP"
      //break //todo: break is not supported

      case Aggregate.VAR_POP =>
      //text = "VAR_POP"
      //break //todo: break is not supported

      case Aggregate.VAR_SAMP =>
      //text = "VAR_SAMP"
      //break //todo: break is not supported

      case Aggregate.BOOL_AND =>
      //text = "BOOL_AND"
      //break //todo: break is not supported

      case Aggregate.BOOL_OR =>
      //text = "BOOL_OR"
      //break //todo: break is not supported

      case _ =>

    }
     null;
  }

}
