package org.apache.spark.sql.math

import scala.math.{abs, min, max, pow}
import annotation.implicitNotFound

import org.apache.spark.sql.types.Decimal

/**
 * @author Erik Osheim
 */


/**
 * Numeric typeclass for doing operations on generic types.
 *
 * Importantly, this package does not deliver classes for you to instantiate.
 * Rather, it gives you a trait to associated with your generic types, which
 * allows actual uses of your generic code with concrete types (e.g. Int) to
 * link up with concrete implementations (e.g. IntIsNumeric) of Numeric's
 * method for that type.
 *
 * @example {{{
 *   import demo.Numeric
 *   import demo.Numeric.FastImplicits._
 *
 *   def pythagoreanTheorem[T:Numeric](a:T, b:T): Double = {
 *     val c = (a * a) + (b * b)
 *     math.sqrt(c.toDouble)
 *   }
 *
 *   def 
 * }}}
 * 
 */
//@implicitNotFound(msg = "Cannot find Numeric type class for ${A}")
trait Numeric[@specialized(Byte, Short, Int,Long,Float,Double) A]
extends ConvertableFrom[A] with ConvertableTo[A] {

  /**
   * Computes the absolute value of `a`.
   * 
   * @return the absolute value of `a`
   */
  def abs(a:A):A

  /**
   * Returns an integer whose sign denotes the relationship between
   * `a` and `b`. If `a` < `b` it returns -1, if `a` == `b` it returns
   * 0 and if `a` > `b` it returns 1.
   *
   * @return -1, 0 or 1
   *
   * @see math.abs
   */
  def compare(a:A, b:A):Int = if (lt(a, b)) -1 else if (gt(a, b)) 1 else 0

  /**
   * Divides `a` by `b`.
   *
   * This method maintains the type of the arguments (`A`). If this
   * method is used with `Int` or `Long` then the quotient (as in
   * integer division). Otherwise (with `Float` and `Double`) a
   * fractional result is returned.
   *
   * @return `a` / `b`
   */
  def div(a:A, b:A):A

  /**
   * Tests if `a` and `b` are equivalent.
   *
   * @return `a` == `b`
   */
  def equiv(a:A, b:A):Boolean

  /**
   * Tests if `a` and `b` are not equivalent.
   *
   * @return `a` != `b`
   */
  def nequiv(a:A, b:A):Boolean

  /**
   * Tests if `a` is greater than `b`.
   *
   * @return `a` > `b`
   */
  def gt(a:A, b:A):Boolean

  /**
   * Tests if `a` is greater than or equal to `b`.
   *
   * @return `a` >= `b`
   */
  def gteq(a:A, b:A):Boolean

  /**
   * Tests if `a` is less than `b`.
   *
   * @return `a` <= `b`
   */
  def lt(a:A, b:A):Boolean

  /**
   * Tests if `a` is less than or equal to `b`.
   *
   * @return `a` <= `b`
   */
  def lteq(a:A, b:A):Boolean

  /**
   * Returns the larger of `a` and `b`.
   *
   * @return max(`a`, `b`)
   *
   * @see math.max
   */
  def max(a:A, b:A):A

  /**
   * Returns the smaller of `a` and `b`.
   *
   * @return min(`a`, `b`)
   *
   * @see math.min
   */
  def min(a:A, b:A):A

  /**
   * Returns `a` minus `b`.
   *
   * @return `a` - `b`
   */
  def minus(a:A, b:A):A

  /**
   * Returns `a` modulo `b`.
   *
   * @return `a` % `b`
   */
  def mod(a:A, b:A):A

  /**
   * Returns the additive inverse `a`.
   *
   * @return -`a`
   */
  def negate(a:A):A

  /**
   * Returns one.
   *
   * @return 1
   */
  def one:A

  /**
   * Returns `a` plus `b`.
   *
   * @return `a` + `b`
   */
  def plus(a:A, b:A):A

  /**
   * Returns `a` to the `b`th power.
   *
   * Note that with large numbers this method will overflow and
   * return Infinity, which becomes MaxValue for whatever type
   * is being used. This behavior is inherited from `math.pow`.
   * 
   * @returns pow(`a`, `b`)
   * 
   * @see math.pow
   */
  def pow(a:A, b:A):A

  /**
   * Returns an integer whose sign denotes the sign of `a`.
   * If `a` is negative it returns -1, if `a` is zero it
   * returns 0 and if `a` is positive it returns 1.
   *
   * @return -1, 0 or 1
   */
  def signum(a:A):Int = compare(a, zero)

  /**
   * Returns `a` times `b`.
   *
   * @return `a` * `b`
   */
  def times(a:A, b:A):A

  /**
   * Returns zero.
   *
   * @return 0
   */
  def zero:A

  /**
   * Convert a value `b` of type `B` to type `A`.
   *
   * This method can be used to coerce one generic numeric type to
   * another, to allow operations on them jointly.
   *
   * @example {{{
   *   def foo[A:Numeric,B:Numeric](a:A, b:B) = {
   *     val n = implicitly[Numeric[A]]
   *     n.add(a, n.fromType(b))
   *   }
   * }}}
   *
   * Note that `b` may lose precision when represented as an `A`
   * (e.g. if B is Long and A is Int).
   * 
   * @return the value of `b` encoded in type `A`
   */
  def fromType[@specialized(Int, Long, Float, Double) B](b:B)(implicit c:ConvertableFrom[B]): A

  def toType[@specialized(Int, Long, Float, Double) B](a:A)(implicit c:ConvertableTo[B]): B

  /**
   * Used to get an Ordering[A] instance.
   */
  def getOrdering():Ordering[A] = new NumericOrdering(this)

  class Ops(lhs: A) {
    def +(rhs: A) = plus(lhs, rhs)
    def -(rhs: A) = minus(lhs, rhs)
    def *(rhs: A) = times(lhs, rhs)
    def unary_-() = negate(lhs)
    def abs(): A = Numeric.this.abs(lhs)
    def signum(): Int = Numeric.this.signum(lhs)
    def toInt(): Int = Numeric.this.toInt(lhs)
    def toLong(): Long = Numeric.this.toLong(lhs)
    def toFloat(): Float = Numeric.this.toFloat(lhs)
    def toDouble(): Double = Numeric.this.toDouble(lhs)
  }

  implicit def mkNumericOps(lhs: A): Ops = new Ops(lhs)
}

/**
 * This is a little helper class that allows us to support the Ordering trait.
 *
 * If Numeric extended Ordering directly then we'd have to override all of
 * the comparison operators, losing specialization and other performance
 * benefits.
 */
class NumericOrdering[A](n:Numeric[A]) extends Ordering[A] {
  def compare(a:A, b:A) = n.compare(a, b)
}

trait ByteIsNumeric
  extends Numeric[Byte] with ConvertableFromByte with ConvertableToByte {
  def abs(a:Byte): Byte = scala.math.abs(a)
  def div(a:Byte, b:Byte): Byte = a / b
  def equiv(a:Byte, b:Byte): Boolean = a == b
  def gt(a:Byte, b:Byte): Boolean = a > b
  def gteq(a:Byte, b:Byte): Boolean = a >= b
  def lt(a:Byte, b:Byte): Boolean = a < b
  def lteq(a:Byte, b:Byte): Boolean = a <= b
  def max(a:Byte, b:Byte): Byte = scala.math.max(a, b)
  def min(a:Byte, b:Byte): Byte = scala.math.min(a, b)
  def minus(a:Byte, b:Byte): Byte = a - b
  def mod(a:Byte, b:Byte): Byte = a % b
  def negate(a:Byte): Byte = -a
  def nequiv(a:Byte, b:Byte): Boolean = a != b
  def one: Byte = 1
  def plus(a:Byte, b:Byte): Byte = a + b
  def pow(a:Byte, b:Byte): Byte = scala.math.pow(a, b).toByte
  def times(a:Byte, b:Byte): Byte = a * b
  def zero: Byte = 0

  def fromType[@specialized(Byte, Short, Int, Long, Float, Double) B](b:B)(implicit c:ConvertableFrom[B]) = c.toByte(b)
  def toType[@specialized(Byte, Short, Int, Long, Float, Double) B](a:Byte)(implicit c:ConvertableTo[B]) = c.fromByte(a)
}

trait ShortIsNumeric
  extends Numeric[Short] with ConvertableFromShort with ConvertableToShort {
  def abs(a:Short): Short = scala.math.abs(a)
  def div(a:Short, b:Short): Short = a / b
  def equiv(a:Short, b:Short): Boolean = a == b
  def gt(a:Short, b:Short): Boolean = a > b
  def gteq(a:Short, b:Short): Boolean = a >= b
  def lt(a:Short, b:Short): Boolean = a < b
  def lteq(a:Short, b:Short): Boolean = a <= b
  def max(a:Short, b:Short): Short = scala.math.max(a, b)
  def min(a:Short, b:Short): Short = scala.math.min(a, b)
  def minus(a:Short, b:Short): Short = a - b
  def mod(a:Short, b:Short): Short = a % b
  def negate(a:Short): Short = -a
  def nequiv(a:Short, b:Short): Boolean = a != b
  def one: Short = 1
  def plus(a:Short, b:Short): Short = a + b
  def pow(a:Short, b:Short): Short = scala.math.pow(a, b).toShort
  def times(a:Short, b:Short): Short = a * b
  def zero: Short = 0

  def fromType[@specialized(Byte, Short, Int, Long, Float, Double) B](b:B)(implicit c:ConvertableFrom[B]) = c.toShort(b)
  def toType[@specialized(Byte, Short, Int, Long, Float, Double) B](a:Short)(implicit c:ConvertableTo[B]) = c.fromShort(a)
}

trait IntIsNumeric
extends Numeric[Int] with ConvertableFromInt with ConvertableToInt {
  def abs(a:Int): Int = scala.math.abs(a)
  def div(a:Int, b:Int): Int = a / b
  def equiv(a:Int, b:Int): Boolean = a == b
  def gt(a:Int, b:Int): Boolean = a > b
  def gteq(a:Int, b:Int): Boolean = a >= b
  def lt(a:Int, b:Int): Boolean = a < b
  def lteq(a:Int, b:Int): Boolean = a <= b
  def max(a:Int, b:Int): Int = scala.math.max(a, b)
  def min(a:Int, b:Int): Int = scala.math.min(a, b)
  def minus(a:Int, b:Int): Int = a - b
  def mod(a:Int, b:Int): Int = a % b
  def negate(a:Int): Int = -a
  def nequiv(a:Int, b:Int): Boolean = a != b
  def one: Int = 1
  def plus(a:Int, b:Int): Int = a + b
  def pow(a:Int, b:Int): Int = scala.math.pow(a, b).toInt
  def times(a:Int, b:Int): Int = a * b
  def zero: Int = 0
  
  def fromType[@specialized(Int, Long, Float, Double) B](b:B)(implicit c:ConvertableFrom[B]) = c.toInt(b)
  def toType[@specialized(Int, Long, Float, Double) B](a:Int)(implicit c:ConvertableTo[B]) = c.fromInt(a)
}

trait LongIsNumeric
extends Numeric[Long] with ConvertableFromLong with ConvertableToLong {
  def abs(a:Long): Long = scala.math.abs(a)
  def div(a:Long, b:Long): Long = a / b
  def equiv(a:Long, b:Long): Boolean = a == b
  def gt(a:Long, b:Long): Boolean = a > b
  def gteq(a:Long, b:Long): Boolean = a >= b
  def lt(a:Long, b:Long): Boolean = a < b
  def lteq(a:Long, b:Long): Boolean = a <= b
  def max(a:Long, b:Long): Long = scala.math.max(a, b)
  def min(a:Long, b:Long): Long = scala.math.min(a, b)
  def minus(a:Long, b:Long): Long = a - b
  def mod(a:Long, b:Long): Long = a % b
  def negate(a:Long): Long = -a
  def nequiv(a:Long, b:Long): Boolean = a != b
  def one: Long = 1L
  def plus(a:Long, b:Long): Long = a + b
  def pow(a:Long, b:Long): Long = scala.math.pow(a, b).toLong
  def times(a:Long, b:Long): Long = a * b
  def zero: Long = 0L
  
  def fromType[@specialized(Int, Long, Float, Double) B](b:B)(implicit c:ConvertableFrom[B]) = c.toLong(b)
  def toType[@specialized(Int, Long, Float, Double) B](a:Long)(implicit c:ConvertableTo[B]) = c.fromLong(a)
}

trait FloatIsNumeric
extends Numeric[Float] with ConvertableFromFloat with ConvertableToFloat {
  def abs(a:Float): Float = scala.math.abs(a)
  def div(a:Float, b:Float): Float = a / b
  def equiv(a:Float, b:Float): Boolean = a == b
  def gt(a:Float, b:Float): Boolean = a > b
  def gteq(a:Float, b:Float): Boolean = a >= b
  def lt(a:Float, b:Float): Boolean = a < b
  def lteq(a:Float, b:Float): Boolean = a <= b
  def max(a:Float, b:Float): Float = scala.math.max(a, b)
  def min(a:Float, b:Float): Float = scala.math.min(a, b)
  def minus(a:Float, b:Float): Float = a - b
  def mod(a:Float, b:Float): Float = a % b
  def negate(a:Float): Float = -a
  def nequiv(a:Float, b:Float): Boolean = a != b
  def one: Float = 1.0F
  def plus(a:Float, b:Float): Float = a + b
  def pow(a:Float, b:Float): Float = scala.math.pow(a, b).toFloat
  def times(a:Float, b:Float): Float = a * b
  def zero: Float = 0.0F
  
  def fromType[@specialized(Int, Long, Float, Double) B](b:B)(implicit c:ConvertableFrom[B]) = c.toFloat(b)
  def toType[@specialized(Int, Long, Float, Double) B](a:Float)(implicit c:ConvertableTo[B]) = c.fromFloat(a)
}

trait DoubleIsNumeric
extends Numeric[Double] with ConvertableFromDouble with ConvertableToDouble {
  def abs(a:Double): Double = scala.math.abs(a)
  def div(a:Double, b:Double): Double = a / b
  def equiv(a:Double, b:Double): Boolean = a == b
  def gt(a:Double, b:Double): Boolean = a > b
  def gteq(a:Double, b:Double): Boolean = a >= b
  def lt(a:Double, b:Double): Boolean = a < b
  def lteq(a:Double, b:Double): Boolean = a <= b
  def max(a:Double, b:Double): Double = scala.math.max(a, b)
  def min(a:Double, b:Double): Double = scala.math.min(a, b)
  def minus(a:Double, b:Double): Double = a - b
  def mod(a:Double, b:Double): Double = a % b
  def negate(a:Double): Double = -a
  def nequiv(a:Double, b:Double): Boolean = a != b
  def one: Double = 1.0
  def plus(a:Double, b:Double): Double = a + b
  def pow(a:Double, b:Double): Double = scala.math.pow(a, b)
  def times(a:Double, b:Double): Double = a * b
  def zero: Double = 0.0
  
  def fromType[@specialized(Int, Long, Float, Double) B](b:B)(implicit c:ConvertableFrom[B]) = c.toDouble(b)
  def toType[@specialized(Int, Long, Float, Double) B](a:Double)(implicit c:ConvertableTo[B]) = c.fromDouble(a)
}

trait BigIntIsNumeric
extends Numeric[BigInt] with ConvertableFromBigInt with ConvertableToBigInt {
  def abs(a:BigInt): BigInt = a.abs
  def div(a:BigInt, b:BigInt): BigInt = a / b
  def equiv(a:BigInt, b:BigInt): Boolean = a == b
  def gt(a:BigInt, b:BigInt): Boolean = a > b
  def gteq(a:BigInt, b:BigInt): Boolean = a >= b
  def lt(a:BigInt, b:BigInt): Boolean = a < b
  def lteq(a:BigInt, b:BigInt): Boolean = a <= b
  def max(a:BigInt, b:BigInt): BigInt = a.max(b)
  def min(a:BigInt, b:BigInt): BigInt = a.min(b)
  def minus(a:BigInt, b:BigInt): BigInt = a - b
  def mod(a:BigInt, b:BigInt): BigInt = a % b
  def negate(a:BigInt): BigInt = -a
  def nequiv(a:BigInt, b:BigInt): Boolean = a != b
  def one: BigInt = BigInt(1)
  def plus(a:BigInt, b:BigInt): BigInt = a + b
  def pow(a:BigInt, b:BigInt): BigInt = a.pow(b)
  def times(a:BigInt, b:BigInt): BigInt = a * b
  def zero: BigInt = BigInt(0)
  
  def fromType[@specialized(Int, Long, Float, Double) B](b:B)(implicit c:ConvertableFrom[B]) = c.toBigInt(b)
  def toType[@specialized(Int, Long, Float, Double) B](a:BigInt)(implicit c:ConvertableTo[B]) = c.fromBigInt(a)
}

trait DecimalIsNumeric
extends Numeric[Decimal] with ConvertableFromDecimal with ConvertableToDecimal {
  def abs(a:Decimal): Decimal = if (lt(a, zero)) negate(a) else a
  def div(a:Decimal, b:Decimal): Decimal = a / b
  def equiv(a:Decimal, b:Decimal): Boolean = a == b
  def gt(a:Decimal, b:Decimal): Boolean = a > b
  def gteq(a:Decimal, b:Decimal): Boolean = a >= b
  def lt(a:Decimal, b:Decimal): Boolean = a < b
  def lteq(a:Decimal, b:Decimal): Boolean = a <= b
  def max(a:Decimal, b:Decimal): Decimal = a.max(b)
  def min(a:Decimal, b:Decimal): Decimal = a.min(b)
  def minus(a:Decimal, b:Decimal): Decimal = a - b
  def mod(a:Decimal, b:Decimal): Decimal = a % b
  def negate(a:Decimal): Decimal = -a
  def nequiv(a:Decimal, b:Decimal): Boolean = a != b
  def one: Decimal = Decimal(1.0)
  def plus(a:Decimal, b:Decimal): Decimal = a + b
  def pow(a:Decimal, b:Decimal): Decimal = a.pow(b)
  def times(a:Decimal, b:Decimal): Decimal = a * b
  def zero: Decimal = Decimal(0.0)
  
  def fromType[@specialized(Int, Long, Float, Double) B](b:B)(implicit c:ConvertableFrom[B]) = c.toDecimal(b)
  def toType[@specialized(Int, Long, Float, Double) B](a:Decimal)(implicit c:ConvertableTo[B]) = c.fromDecimal(a)
}


/**
 * This companion object provides the instances (e.g. IntIsNumeric)
 * associating the type class (Numeric) with its member type (Int).
 */
object Numeric {
  implicit object ByteIsNumeric extends ByteIsNumeric
  implicit object ShortIsNumeric extends ShortIsNumeric
  implicit object IntIsNumeric extends IntIsNumeric
  implicit object LongIsNumeric extends LongIsNumeric
  implicit object FloatIsNumeric extends FloatIsNumeric
  implicit object DoubleIsNumeric extends DoubleIsNumeric
  implicit object BigIntIsNumeric extends BigIntIsNumeric
  implicit object DecimalIsNumeric extends DecimalIsNumeric

  def numeric[@specialized(Byte, Short, Int, Long, Float, Double) A:Numeric]:Numeric[A] = implicitly[Numeric[A]]
}
/*
object FastImplicits {
  implicit def infixOps[@specialized(Int, Long, Float, Double) A:Numeric](a:A) = new FastNumericOps(a)

  implicit def infixIntOps(i:Int) = new LiteralIntOps(i)
  implicit def infixLongOps(l:Long) = new LiteralLongOps(l)
  implicit def infixFloatOps(f:Float) = new LiteralFloatOps(f)
  implicit def infixDoubleOps(d:Double) = new LiteralDoubleOps(d)
  implicit def infixBigIntOps(f:BigInt) = new LiteralBigIntOps(f)
  implicit def infixDecimalOps(d:Decimal) = new LiteralDecimalOps(d)

  def numeric[@specialized(Int, Long, Float, Double) A:Numeric]:Numeric[A] = implicitly[Numeric[A]]  
}

object EasyImplicits {
  implicit def infixOps[@specialized(Int, Long, Float, Double) A:Numeric](a:A) = new EasyNumericOps(a)

  implicit def infixIntOps(i:Int) = new LiteralIntOps(i)
  implicit def infixLongOps(l:Long) = new LiteralLongOps(l)
  implicit def infixFloatOps(f:Float) = new LiteralFloatOps(f)
  implicit def infixDoubleOps(d:Double) = new LiteralDoubleOps(d)
  implicit def infixBigIntOps(f:BigInt) = new LiteralBigIntOps(f)
  implicit def infixDecimalOps(d:Decimal) = new LiteralDecimalOps(d)

  def numeric[@specialized(Int, Long, Float, Double) A:Numeric]:Numeric[A] = implicitly[Numeric[A]]  
}
*/
