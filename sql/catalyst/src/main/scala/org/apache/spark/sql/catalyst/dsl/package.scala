/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst

import java.sql.{Date, Timestamp}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.{TypeTag, typeTag}

import org.apache.spark.sql.catalyst.analysis.{EliminateSubQueries, UnresolvedGetField, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType}
import org.apache.spark.sql.types._

/**
 * A collection of implicit conversions that create a DSL for constructing catalyst data structures.
 *
 * {{{
 *  scala> import org.apache.spark.sql.catalyst.dsl.expressions._
 *
 *  // Standard operators are added to expressions.
 *  scala> import org.apache.spark.sql.catalyst.expressions.Literal
 *  scala> Literal(1) + Literal(1)
 *  res0: org.apache.spark.sql.catalyst.expressions.Add = (1 + 1)
 *
 *  // There is a conversion from 'symbols to unresolved attributes.
 *  scala> 'a.attr
 *  res1: org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute = 'a
 *
 *  // These unresolved attributes can be used to create more complicated expressions.
 *  scala> 'a === 'b
 *  res2: org.apache.spark.sql.catalyst.expressions.EqualTo = ('a = 'b)
 *
 *  // SQL verbs can be used to construct logical query plans.
 *  scala> import org.apache.spark.sql.catalyst.plans.logical._
 *  scala> import org.apache.spark.sql.catalyst.dsl.plans._
 *  scala> LocalRelation('key.int, 'value.string).where('key === 1).select('value).analyze
 *  res3: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan =
 *  Project [value#3]
 *   Filter (key#2 = 1)
 *    LocalRelation [key#2,value#3], []
 * }}}
 */
package object dsl {
  trait ImplicitOperators {
    def expr: Expression

    def unary_- : Expression= UnaryMinus(expr)
    def unary_! : Predicate = Not(expr)
    def unary_~ : Expression = BitwiseNot(expr)

    def + (other: Expression): Expression = Add(expr, other)
    def - (other: Expression): Expression = Subtract(expr, other)
    def * (other: Expression): Expression = Multiply(expr, other)
    def / (other: Expression): Expression = Divide(expr, other)
    def % (other: Expression): Expression = Remainder(expr, other)
    def & (other: Expression): Expression = BitwiseAnd(expr, other)
    def | (other: Expression): Expression = BitwiseOr(expr, other)
    def ^ (other: Expression): Expression = BitwiseXor(expr, other)

    def && (other: Expression): Predicate = And(expr, other)
    def || (other: Expression): Predicate = Or(expr, other)

    def < (other: Expression): Predicate = LessThan(expr, other)
    def <= (other: Expression): Predicate = LessThanOrEqual(expr, other)
    def > (other: Expression): Predicate = GreaterThan(expr, other)
    def >= (other: Expression): Predicate = GreaterThanOrEqual(expr, other)
    def === (other: Expression): Predicate = EqualTo(expr, other)
    def <=> (other: Expression): Predicate = EqualNullSafe(expr, other)
    def !== (other: Expression): Predicate = Not(EqualTo(expr, other))

    def in(list: Expression*): Expression = In(expr, list)

    def like(other: Expression): Expression = Like(expr, other)
    def rlike(other: Expression): Expression = RLike(expr, other)
    def contains(other: Expression): Expression = Contains(expr, other)
    def startsWith(other: Expression): Expression = StartsWith(expr, other)
    def endsWith(other: Expression): Expression = EndsWith(expr, other)
    def substr(pos: Expression, len: Expression = Literal(Int.MaxValue)): Expression =
      Substring(expr, pos, len)
    def substring(pos: Expression, len: Expression = Literal(Int.MaxValue)): Expression =
      Substring(expr, pos, len)

    def isNull: Predicate = IsNull(expr)
    def isNotNull: Predicate = IsNotNull(expr)

    def getItem(ordinal: Expression): Expression = GetItem(expr, ordinal)
    def getField(fieldName: String): UnresolvedGetField = UnresolvedGetField(expr, fieldName)

    def cast(to: DataType): Expression = Cast(expr, to)

    def asc: SortOrder = SortOrder(expr, Ascending)
    def desc: SortOrder = SortOrder(expr, Descending)

    def as(alias: String): NamedExpression = Alias(expr, alias)()
    def as(alias: Symbol): NamedExpression = Alias(expr, alias.name)()
  }

  trait ExpressionConversions {
    implicit class DslExpression(e: Expression) extends ImplicitOperators {
      def expr: Expression = e
    }

    implicit def booleanToLiteral(b: Boolean): Literal = Literal(b)
    implicit def byteToLiteral(b: Byte): Literal = Literal(b)
    implicit def shortToLiteral(s: Short): Literal = Literal(s)
    implicit def intToLiteral(i: Int): Literal = Literal(i)
    implicit def longToLiteral(l: Long): Literal = Literal(l)
    implicit def floatToLiteral(f: Float): Literal = Literal(f)
    implicit def doubleToLiteral(d: Double): Literal = Literal(d)
    implicit def stringToLiteral(s: String): Literal = Literal(s)
    implicit def dateToLiteral(d: Date): Literal = Literal(d)
    implicit def bigDecimalToLiteral(d: BigDecimal): Literal = Literal(d.underlying())
    implicit def bigDecimalToLiteral(d: java.math.BigDecimal): Literal = Literal(d)
    implicit def decimalToLiteral(d: Decimal): Literal = Literal(d)
    implicit def timestampToLiteral(t: Timestamp): Literal = Literal(t)
    implicit def binaryToLiteral(a: Array[Byte]): Literal = Literal(a)

    implicit def symbolToUnresolvedAttribute(s: Symbol): analysis.UnresolvedAttribute =
      analysis.UnresolvedAttribute(s.name)

    /** Converts $"col name" into an [[analysis.UnresolvedAttribute]]. */
    implicit class StringToAttributeConversionHelper(val sc: StringContext) {
      // Note that if we make ExpressionConversions an object rather than a trait, we can
      // then make this a value class to avoid the small penalty of runtime instantiation.
      def $(args: Any*): analysis.UnresolvedAttribute = {
        analysis.UnresolvedAttribute(sc.s(args :_*))
      }
    }

    def sum(e: Expression): Expression = Sum(e)
    def sumDistinct(e: Expression): Expression = SumDistinct(e)
    def count(e: Expression): Expression = Count(e)
    def countDistinct(e: Expression*): Expression = CountDistinct(e)
    def approxCountDistinct(e: Expression, rsd: Double = 0.05): Expression =
      ApproxCountDistinct(e, rsd)
    def avg(e: Expression): Expression = Average(e)
    def first(e: Expression): Expression = First(e)
    def last(e: Expression): Expression = Last(e)
    def min(e: Expression): Expression = Min(e)
    def max(e: Expression): Expression = Max(e)
    def upper(e: Expression): Expression = Upper(e)
    def lower(e: Expression): Expression = Lower(e)
    def sqrt(e: Expression): Expression = Sqrt(e)
    def abs(e: Expression): Expression = Abs(e)

    implicit class DslSymbol(sym: Symbol) extends ImplicitAttribute { def s: String = sym.name }
    // TODO more implicit class for literal?
    implicit class DslString(val s: String) extends ImplicitOperators {
      override def expr: Expression = Literal(s)
      def attr: UnresolvedAttribute = analysis.UnresolvedAttribute(s)
    }

    abstract class ImplicitAttribute extends ImplicitOperators {
      def s: String
      def expr: UnresolvedAttribute = attr
      def attr: UnresolvedAttribute = analysis.UnresolvedAttribute(s)

      /** Creates a new AttributeReference of type boolean */
      def boolean: AttributeReference = AttributeReference(s, BooleanType, nullable = true)()

      /** Creates a new AttributeReference of type byte */
      def byte: AttributeReference = AttributeReference(s, ByteType, nullable = true)()

      /** Creates a new AttributeReference of type short */
      def short: AttributeReference = AttributeReference(s, ShortType, nullable = true)()

      /** Creates a new AttributeReference of type int */
      def int: AttributeReference = AttributeReference(s, IntegerType, nullable = true)()

      /** Creates a new AttributeReference of type long */
      def long: AttributeReference = AttributeReference(s, LongType, nullable = true)()

      /** Creates a new AttributeReference of type float */
      def float: AttributeReference = AttributeReference(s, FloatType, nullable = true)()

      /** Creates a new AttributeReference of type double */
      def double: AttributeReference = AttributeReference(s, DoubleType, nullable = true)()

      /** Creates a new AttributeReference of type string */
      def string: AttributeReference = AttributeReference(s, StringType, nullable = true)()

      /** Creates a new AttributeReference of type date */
      def date: AttributeReference = AttributeReference(s, DateType, nullable = true)()

      /** Creates a new AttributeReference of type decimal */
      def decimal: AttributeReference =
        AttributeReference(s, DecimalType.Unlimited, nullable = true)()

      /** Creates a new AttributeReference of type decimal */
      def decimal(precision: Int, scale: Int): AttributeReference =
        AttributeReference(s, DecimalType(precision, scale), nullable = true)()

      /** Creates a new AttributeReference of type timestamp */
      def timestamp: AttributeReference = AttributeReference(s, TimestampType, nullable = true)()

      /** Creates a new AttributeReference of type binary */
      def binary: AttributeReference = AttributeReference(s, BinaryType, nullable = true)()

      /** Creates a new AttributeReference of type array */
      def array(dataType: DataType): AttributeReference =
        AttributeReference(s, ArrayType(dataType), nullable = true)()

      /** Creates a new AttributeReference of type map */
      def map(keyType: DataType, valueType: DataType): AttributeReference =
        map(MapType(keyType, valueType))

      def map(mapType: MapType): AttributeReference =
        AttributeReference(s, mapType, nullable = true)()

      /** Creates a new AttributeReference of type struct */
      def struct(fields: StructField*): AttributeReference = struct(StructType(fields))
      def struct(structType: StructType): AttributeReference =
        AttributeReference(s, structType, nullable = true)()
    }

    implicit class DslAttribute(a: AttributeReference) {
      def notNull: AttributeReference = a.withNullability(false)
      def nullable: AttributeReference = a.withNullability(true)

      // Protobuf terminology
      def required: AttributeReference = a.withNullability(false)

      def at(ordinal: Int): BoundReference = BoundReference(ordinal, a.dataType, a.nullable)
    }
  }


  object expressions extends ExpressionConversions  // scalastyle:ignore

  abstract class LogicalPlanFunctions {
    def logicalPlan: LogicalPlan

    def select(exprs: NamedExpression*): LogicalPlan = Project(exprs, logicalPlan)

    def where(condition: Expression): LogicalPlan = Filter(condition, logicalPlan)

    def limit(limitExpr: Expression): LogicalPlan = Limit(limitExpr, logicalPlan)

    def join(
        otherPlan: LogicalPlan,
        joinType: JoinType = Inner,
        condition: Option[Expression] = None): LogicalPlan =
      Join(logicalPlan, otherPlan, joinType, condition)

    def orderBy(sortExprs: SortOrder*): LogicalPlan = Sort(sortExprs, true, logicalPlan)

    def sortBy(sortExprs: SortOrder*): LogicalPlan = Sort(sortExprs, false, logicalPlan)

    def groupBy(groupingExprs: Expression*)(aggregateExprs: Expression*): LogicalPlan = {
      val aliasedExprs = aggregateExprs.map {
        case ne: NamedExpression => ne
        case e => Alias(e, e.toString)()
      }
      Aggregate(groupingExprs, aliasedExprs, logicalPlan)
    }

    def subquery(alias: Symbol): LogicalPlan = Subquery(alias.name, logicalPlan)

    def unionAll(otherPlan: LogicalPlan): LogicalPlan = Union(logicalPlan, otherPlan)

    def sfilter[T1](arg1: Symbol)(udf: (T1) => Boolean): LogicalPlan =
      Filter(ScalaUdf(udf, BooleanType, Seq(UnresolvedAttribute(arg1.name))), logicalPlan)

    def sample(
        fraction: Double,
        withReplacement: Boolean = true,
        seed: Int = (math.random * 1000).toInt): LogicalPlan =
      Sample(fraction, withReplacement, seed, logicalPlan)

    // TODO specify the output column names
    def generate(
        generator: Generator,
        join: Boolean = false,
        outer: Boolean = false,
        alias: Option[String] = None): LogicalPlan =
      Generate(generator, join = join, outer = outer, alias, Nil, logicalPlan)

    def insertInto(tableName: String, overwrite: Boolean = false): LogicalPlan =
      InsertIntoTable(
        analysis.UnresolvedRelation(Seq(tableName)), Map.empty, logicalPlan, overwrite, false)

    def analyze: LogicalPlan = EliminateSubQueries(analysis.SimpleAnalyzer(logicalPlan))
  }

  object plans {  // scalastyle:ignore
    implicit class DslLogicalPlan(val logicalPlan: LogicalPlan) extends LogicalPlanFunctions {
      def writeToFile(path: String): LogicalPlan = WriteToFile(path, logicalPlan)
    }
  }

  case class ScalaUdfBuilder[T: TypeTag](f: AnyRef) {
    def call(args: Expression*): ScalaUdf = {
      ScalaUdf(f, ScalaReflection.schemaFor(typeTag[T]).dataType, args)
    }
  }

  // scalastyle:off
  /** functionToUdfBuilder 1-22 were generated by this script

    (1 to 22).map { x =>
      val argTypes = Seq.fill(x)("_").mkString(", ")
      s"implicit def functionToUdfBuilder[T: TypeTag](func: Function$x[$argTypes, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)"
    }
  */

  implicit def functionToUdfBuilder[T: TypeTag](func: Function1[_, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function2[_, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function3[_, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function4[_, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function5[_, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function6[_, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function7[_, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function8[_, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function9[_, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function10[_, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function11[_, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function12[_, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function13[_, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)

  implicit def functionToUdfBuilder[T: TypeTag](func: Function22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, T]): ScalaUdfBuilder[T] = ScalaUdfBuilder(func)
  // scalastyle:on
}
