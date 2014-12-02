package org.apache.spark.sql.hive.h2

import org.apache.spark.sql.catalyst.expressions.{Literal, Expression}
import org.apache.spark.sql.catalyst.types.{StringType, IntegerType}
import org.h2.expression.ValueExpression
import org.h2.message.DbException
import org.h2.value.Value

/**
 * Created by w00297350 on 2014/12/2.
 */
object ValueParser {

  def apply(value :Value):Expression=
  {
    val valueType: Int = value.getType

    valueType match {
      case Value.BYTE =>

      case Value.SHORT =>

      case Value.INT =>

        val intValue: Int = value.getInt
        val literal: Literal = new Literal(intValue, IntegerType)
        return literal

      case Value.LONG =>

      case Value.DECIMAL =>

      case Value.TIME =>

      case Value.DATE =>

      case Value.TIMESTAMP =>

      case Value.BYTES =>

      case Value.JAVA_OBJECT =>

      case Value.STRING =>

        val intValue = value.getString
        val literal: Literal = new Literal(intValue, StringType)
        return literal

      case Value.STRING_IGNORECASE =>

      case Value.STRING_FIXED =>

      case Value.DOUBLE =>

      case Value.FLOAT =>

      case Value.CLOB =>

      case Value.BLOB =>

      case Value.ARRAY =>

      case Value.RESULT_SET =>

      case Value.UUID =>

      case Value.GEOMETRY =>

      case _ =>
        throw DbException.throwInternalError("type=" + valueType)
    }
    sys.error(s"Unsupported h2 sql value parser: $value")
  }

}
