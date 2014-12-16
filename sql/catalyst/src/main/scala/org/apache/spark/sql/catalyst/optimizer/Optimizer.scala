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

package org.apache.spark.sql.catalyst.optimizer

import java.sql.Date

import scala.collection.immutable.HashSet
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.decimal.Decimal

import scala.collection.mutable.ArrayBuffer

abstract class Optimizer extends RuleExecutor[LogicalPlan]

object DefaultOptimizer extends Optimizer {
  val batches =
    Batch("Combine Limits", FixedPoint(100),
      CombineLimits) ::
    Batch("ConstantFolding", FixedPoint(100),
      NullPropagation,
      ConstantFolding,
      LikeSimplification,
      ConditionSimplification,
      BooleanSimplification,
      SimplifyFilters,
      SimplifyCasts,
      SimplifyCaseConversionExpressions,
      OptimizeIn) ::
    Batch("Decimal Optimizations", FixedPoint(100),
      DecimalAggregates) ::
    Batch("Filter Pushdown", FixedPoint(100),
      UnionPushdown,
      CombineFilters,
      PushPredicateThroughProject,
      PushPredicateThroughJoin,
      ColumnPruning) :: Nil
}

/**
  *  Pushes operations to either side of a Union.
  */
object UnionPushdown extends Rule[LogicalPlan] {

  /**
    *  Maps Attributes from the left side to the corresponding Attribute on the right side.
    */
  def buildRewrites(union: Union): AttributeMap[Attribute] = {
    assert(union.left.output.size == union.right.output.size)

    AttributeMap(union.left.output.zip(union.right.output))
  }

  /**
    *  Rewrites an expression so that it can be pushed to the right side of a Union operator.
    *  This method relies on the fact that the output attributes of a union are always equal
    *  to the left child's output.
    */
  def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]): A = {
    val result = e transform {
      case a: Attribute => rewrites(a)
    }

    // We must promise the compiler that we did not discard the names in the case of project
    // expressions.  This is safe since the only transformation is from Attribute => Attribute.
    result.asInstanceOf[A]
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Push down filter into union
    case Filter(condition, u @ Union(left, right)) =>
      val rewrites = buildRewrites(u)
      Union(
        Filter(condition, left),
        Filter(pushToRight(condition, rewrites), right))

    // Push down projection into union
    case Project(projectList, u @ Union(left, right)) =>
      val rewrites = buildRewrites(u)
      Union(
        Project(projectList, left),
        Project(projectList.map(pushToRight(_, rewrites)), right))
  }
}


/**
 * Attempts to eliminate the reading of unneeded columns from the query plan using the following
 * transformations:
 *
 *  - Inserting Projections beneath the following operators:
 *   - Aggregate
 *   - Project <- Join
 *   - LeftSemiJoin
 *  - Collapse adjacent projections, performing alias substitution.
 */
object ColumnPruning extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Eliminate attributes that are not needed to calculate the specified aggregates.
    case a @ Aggregate(_, _, child) if (child.outputSet -- a.references).nonEmpty =>
      a.copy(child = Project(a.references.toSeq, child))

    // Eliminate unneeded attributes from either side of a Join.
    case Project(projectList, Join(left, right, joinType, condition)) =>
      // Collect the list of all references required either above or to evaluate the condition.
      val allReferences: AttributeSet =
        AttributeSet(
          projectList.flatMap(_.references.iterator)) ++
          condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      /** Applies a projection only when the child is producing unnecessary attributes */
      def pruneJoinChild(c: LogicalPlan) = prunedChild(c, allReferences)

      Project(projectList, Join(pruneJoinChild(left), pruneJoinChild(right), joinType, condition))

    // Eliminate unneeded attributes from right side of a LeftSemiJoin.
    case Join(left, right, LeftSemi, condition) =>
      // Collect the list of all references required to evaluate the condition.
      val allReferences: AttributeSet =
        condition.map(_.references).getOrElse(AttributeSet(Seq.empty))

      Join(left, prunedChild(right, allReferences), LeftSemi, condition)

    // Combine adjacent Projects.
    case Project(projectList1, Project(projectList2, child)) =>
      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
      val aliasMap = projectList2.collect {
        case a @ Alias(e, _) => (a.toAttribute: Expression, a)
      }.toMap

      // Substitute any attributes that are produced by the child projection, so that we safely
      // eliminate it.
      // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
      // TODO: Fix TransformBase to avoid the cast below.
      val substitutedProjection = projectList1.map(_.transform {
        case a if aliasMap.contains(a) => aliasMap(a)
      }).asInstanceOf[Seq[NamedExpression]]

      Project(substitutedProjection, child)

    // Eliminate no-op Projects
    case Project(projectList, child) if child.output == projectList => child
  }

  /** Applies a projection only when the child is producing unnecessary attributes */
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if ((c.outputSet -- allReferences.filter(c.outputSet.contains)).nonEmpty) {
      Project(allReferences.filter(c.outputSet.contains).toSeq, c)
    } else {
      c
    }
}

/**
 * Simplifies LIKE expressions that do not need full regular expressions to evaluate the condition.
 * For example, when the expression is just checking to see if a string starts with a given
 * pattern.
 */
object LikeSimplification extends Rule[LogicalPlan] {
  // if guards below protect from escapes on trailing %.
  // Cases like "something\%" are not optimized, but this does not affect correctness.
  val startsWith = "([^_%]+)%".r
  val endsWith = "%([^_%]+)".r
  val contains = "%([^_%]+)%".r
  val equalTo = "([^_%]*)".r

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Like(l, Literal(startsWith(pattern), StringType)) if !pattern.endsWith("\\") =>
      StartsWith(l, Literal(pattern))
    case Like(l, Literal(endsWith(pattern), StringType)) =>
      EndsWith(l, Literal(pattern))
    case Like(l, Literal(contains(pattern), StringType)) if !pattern.endsWith("\\") =>
      Contains(l, Literal(pattern))
    case Like(l, Literal(equalTo(pattern), StringType)) =>
      EqualTo(l, Literal(pattern))
  }
}

/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values. This rule is more specific with
 * Null value propagation from bottom to top of the expression tree.
 */
object NullPropagation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case e @ Count(Literal(null, _)) => Cast(Literal(0L), e.dataType)
      case e @ Sum(Literal(c, _)) if c == 0 => Cast(Literal(0L), e.dataType)
      case e @ Average(Literal(c, _)) if c == 0 => Literal(0.0, e.dataType)
      case e @ IsNull(c) if !c.nullable => Literal(false, BooleanType)
      case e @ IsNotNull(c) if !c.nullable => Literal(true, BooleanType)
      case e @ GetItem(Literal(null, _), _) => Literal(null, e.dataType)
      case e @ GetItem(_, Literal(null, _)) => Literal(null, e.dataType)
      case e @ GetField(Literal(null, _), _) => Literal(null, e.dataType)
      case e @ EqualNullSafe(Literal(null, _), r) => IsNull(r)
      case e @ EqualNullSafe(l, Literal(null, _)) => IsNull(l)

      // For Coalesce, remove null literals.
      case e @ Coalesce(children) =>
        val newChildren = children.filter {
          case Literal(null, _) => false
          case _ => true
        }
        if (newChildren.length == 0) {
          Literal(null, e.dataType)
        } else if (newChildren.length == 1) {
          newChildren(0)
        } else {
          Coalesce(newChildren)
        }

      case e @ Substring(Literal(null, _), _, _) => Literal(null, e.dataType)
      case e @ Substring(_, Literal(null, _), _) => Literal(null, e.dataType)
      case e @ Substring(_, _, Literal(null, _)) => Literal(null, e.dataType)

      // Put exceptional cases above if any
      case e: BinaryArithmetic => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
      case e: BinaryComparison => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
      case e: StringRegexExpression => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
      case e: StringComparison => e.children match {
        case Literal(null, _) :: right :: Nil => Literal(null, e.dataType)
        case left :: Literal(null, _) :: Nil => Literal(null, e.dataType)
        case _ => e
      }
    }
  }
}

/**
 * Replaces [[Expression Expressions]] that can be statically evaluated with
 * equivalent [[Literal]] values.
 */
object ConstantFolding extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      // Skip redundant folding of literals. This rule is technically not necessary. Placing this
      // here avoids running the next rule for Literal values, which would create a new Literal
      // object and running eval unnecessarily.
      case l: Literal => l

      // Fold expressions that are foldable.
      case e if e.foldable => Literal(e.eval(null), e.dataType)

      // Fold "literal in (item1, item2, ..., literal, ...)" into true directly.
      case In(Literal(v, _), list) if list.exists {
          case Literal(candidate, _) if candidate == v => true
          case _ => false
        } => Literal(true, BooleanType)
    }
  }
}

/**
 * Replaces [[In (value, seq[Literal])]] with optimized version[[InSet (value, HashSet[Literal])]]
 * which is much faster
 */
object OptimizeIn extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case In(v, list) if !list.exists(!_.isInstanceOf[Literal]) =>
          val hSet = list.map(e => e.eval(null))
          InSet(v, HashSet() ++ hSet)
    }
  }
}

object ConditionSimplification extends Rule[LogicalPlan] {
  import BinaryComparison.LiteralComparison

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown  {
      case origin: CombinePredicate =>
        origin.toOptimized
    }
  }

  case class SplitUnit(unit: Expression, remain: Expression)

  implicit class CombinePredicateExtension(source: CombinePredicate) {
    def find(goal: Expression): Boolean = {
      def delegate(child: Expression): Boolean = (child, goal) match {
        case (combine: CombinePredicate, _) =>
          isSameCombinePredicate(source, combine) && combine.find(goal)

         // if left child is a literal
         // LiteralComparison's unapply method change the literal and attribute position
        case (LiteralComparison(childComparison), LiteralComparison(goalComparison)) =>
          isSame(childComparison, goalComparison)

        case other =>
          isSame(child, goal)
      }

      // using method to avoid right side compute if left side is true
      val leftResult = () => delegate(source.left)
      val rightResult = () => delegate(source.right)
      leftResult() || rightResult()
    }

    @inline
    def isOrPredicate: Boolean = {
      source.isInstanceOf[Or]
    }

    // create a new combine predicate that has the same combine operator as this
    @inline
    def build(left: Expression, right: Expression): CombinePredicate = {
      CombinePredicate(left, right, isOrPredicate)
    }

    // swap left child and right child
    @inline
    def swap: CombinePredicate = {
      source.build(source.right, source.left)
    }

    // Example : a || b || (c && d) => (a, b || (c && d)) or (b, a || (c && d)) or (c && d, a || b)
    def split: Seq[SplitUnit] = {
      def process(child: Expression, others: Expression): Seq[SplitUnit] = {
        child match {
          case combine: CombinePredicate if isSameCombinePredicate(source, combine)=>
            combine.split.map(unit =>
              SplitUnit(unit.unit, source.build(unit.remain, others)))

          case other =>
            Seq(SplitUnit(child, others))
        }
      }
      val leftUnits = process(source.left, source.right)
      val rightUnits = process(source.right, source.left)
      leftUnits ++ rightUnits
    }

    def toOptimized: Expression = source match {
      // predicate is || or &&, left equals right , drop right, keep left
      // if left is also || or &&, optimize it
      case CombinePredicate(left, right) if left.fastEquals(right) =>
        if (isCombinePredicate(left)) {
          left.toCombinePredicate.toOptimized
        } else {
          left
        }

      // left and right are both binary comparison that contains literal
      // here LiteralComparison unapply method changed the child position
      // if there is no combination, should return the origin one
      case origin @ CombinePredicate(LiteralComparison(left), LiteralComparison(right)) =>
        val changed = origin.build(left, right)
        val optimized = changed.mergeComparison
        if (isSame(changed, optimized)) {
          origin
        } else {
          optimized
        }

      case origin @ CombinePredicate(left @ CombinePredicate(left1, right1), right)
        if isNotCombinePredicate(left1, right1, right) =>
        val leftOptimized = left.toOptimized
        if (isSame(left, leftOptimized)) {
          if (isSame(left1, right) || isSame(right1, right)) {
            if (isSameCombinePredicate(origin, left)) leftOptimized else right
          } else {
            val left1Right = origin.build(left1, right)
            val right1Right = origin.build(right1, right)
            val optimizedLR = left1Right.toOptimized
            val optimizedRR = right1Right.toOptimized
            if (isSame(left1Right, optimizedLR) && isSame(right1Right, optimizedRR)) {
              origin
            } else if ((isNotCombinePredicate(optimizedLR, optimizedRR))
              || isSameCombinePredicate(origin, left)) {
              left.build(optimizedLR, optimizedRR).toOptimized
            } else if (optimizedLR.isLiteral || optimizedRR.isLiteral) {
              left.build(optimizedLR, optimizedRR)
            } else {
              origin
            }
          }
        } else if (isNotCombinePredicate(leftOptimized)) {
          origin.build(leftOptimized, right).toOptimized
        } else {
          origin
        }

      case origin @ CombinePredicate(left, right @ CombinePredicate(left2, right2))
        if isNotCombinePredicate(left, left2, right2) =>
        val changed = origin.swap
        val optimized = changed.toOptimized
        if (isSame(changed, optimized)) {
          origin
        } else {
          optimized
        }

      // do optimize like : (a || b || c)  && a => a, here a, b , c is a condition
      case origin @ CombinePredicate(left @ CombinePredicate(ll, lr), right)
        if isNotCombinePredicate(right) =>
        val leftOptimized = left.toOptimized
        if (isSame(left, leftOptimized)) {
          if (left.find(right)) {
            if (isSameCombinePredicate(origin, left)) left else right
          } else {
            // process more complicated case ?
            origin
          }
        } else {
          origin.build(leftOptimized, right).toOptimized
        }

      case origin @ CombinePredicate(left, right @ CombinePredicate(rl, rr))
        if isNotCombinePredicate(left) =>
        val changed = origin.swap
        val optimized = changed.toOptimized
        if (isSame(changed, optimized)) {
          origin
        } else {
          optimized
        }

      case origin @ CombinePredicate(left: CombinePredicate,
                                    right: CombinePredicate)  =>
        val leftOptimized = left.toOptimized
        val rightOptimized = right.toOptimized

        if (isSame(left, leftOptimized) && isSame(right, rightOptimized)) {
          val leftUnits = left.split
          val rightUnits = right.split
          for (leftUnit <- leftUnits) {
            for (rightUnit <- rightUnits) {
              if (isSame(leftUnit.unit, rightUnit.unit)) {
                if (isSameCombinePredicate(origin, left, right)) {
                  return origin.build(leftUnit.remain, right).toOptimized
                } else if (isSameCombinePredicate(origin, right)) {
                  return right
                } else if (isSameCombinePredicate(origin, left)) {
                  return left
                } else {
                  val remains = origin.build(leftUnit.remain, rightUnit.remain).toOptimized
                  return left.build(leftUnit.unit, remains)
                }
              }
            }
          }
          // TODO : process more complicated case
          origin
        } else {
          origin.build(leftOptimized, rightOptimized).toOptimized
        }

      case other =>
        other
    }
    // merge to literal comparison(contains literal in binary comparison)
    // here assume two children both are `BinaryComparison`
    // TODO : find a way to simplify the code O_o
    def mergeComparison: Expression = {
      val left = source.left.toBinaryComparison
      val right = source.right.toBinaryComparison
      if (!isSame(left.left, right.left)) {
        source
      } else {
        (left, right) match {
          case (BinaryComparison(attr, Literal(vLeft, tLeft @ NativeType())),
          BinaryComparison(_, Literal(vRight, tRight @ NativeType()))) =>
            val result = compare(vLeft, vRight)
            result.filter(_ > 0).map(c => {
              if (((left.isLess || left.isLessEquals)
                && (right.isLess || right.isLessEquals || right.isEquals))) {
                if (isOrPredicate) left else right
              } else if ((left.isLess || left.isLessEquals)
                && (right.isGreater || right.isGreaterEquals)) {
                if (isOrPredicate) Literal(true, BooleanType) else source
              } else if ((left.isEquals || left.isGreater || left.isGreaterEquals)
                && (right.isLess || right.isLessEquals || right.isEquals)) {
                if (isOrPredicate) source else Literal(false, BooleanType)
              } else if (left.isEquals && (right.isGreater || right.isGreaterEquals)) {
                if (isOrPredicate) right else left
              } else if ((left.isGreater || left.isGreaterEquals)
                && (right.isLess || right.isEquals || right.isLessEquals)) {
                if (isOrPredicate) source else Literal(false, BooleanType)
              } else {
                if (isOrPredicate) right else left
              }
            }).getOrElse(result.filter(_ == 0).map(c => {
              if (isOrPredicate) {
                if ((left.symbol == right.symbol)
                  || (left.isLessEquals && (right.isLess || right.isEquals))
                  || (left.isGreaterEquals && (right.isGreater || right.isEquals))){
                  left
                } else if(((left.isEquals || left.isLess) && right.isLessEquals)
                  || ((left.isEquals || left.isGreater) && right.isGreaterEquals)) {
                  right
                } else if ((left.isEquals && right.isLess)
                  || (right.isEquals && left.isLess)) {
                  LessThanOrEqual(attr, Literal(vLeft, tLeft))
                } else if ((left.isEquals && right.isGreater)
                  || (right.isEquals && left.isGreater)){
                  GreaterThanOrEqual(attr, Literal(vLeft, tLeft))
                } else if((left.isLess && right.isGreater)
                  || (left.isGreater && right.isLess)) {
                  Not(EqualTo(attr, Literal(vLeft, tRight)))
                }else {
                  Literal(true, BooleanType)
                }
              } else {
                if ((left.symbol == right.symbol)
                  || (left.isLess && right.isLessEquals)
                  || (left.isGreater && right.isGreaterEquals)) {
                  left
                } else if((left.isEquals && (right.isLessEquals || right.isGreaterEquals))
                  || (right.isEquals && (left.isLessEquals || left.isGreaterEquals))) {
                  EqualTo(attr, Literal(vLeft, tLeft))
                } else if ((left.isLessEquals && right.isLess)
                  || (left.isGreaterEquals && right.isGreater)) {
                  right
                } else {
                  Literal(false, BooleanType)
                }
              }
            }).getOrElse(result.filter(_ < 0).map(c => {
              // if there is no combination, return the origin one
              // since `fastEqual` is not equals when `A.left = B.right && A.right = B.left`
              val changed = build(right, left)
              val optimized = changed.mergeComparison
              if (changed.fastEquals(optimized)) {
                source
              } else {
                optimized
              }
            }).getOrElse(source)))

          case _ =>
            source
        }
      }
    }
  }

  implicit class ExpressionCookies(expression: Expression) {
    @inline
    def isLiteral = expression.isInstanceOf[Literal]

    @inline
    def toCombinePredicate = expression.asInstanceOf[CombinePredicate]

    @inline
    def toBinaryComparison = expression.asInstanceOf[BinaryComparison]

    @inline
    def isLess: Boolean = {
      expression.isInstanceOf[LessThan]
    }

    @inline
    def isLessEquals: Boolean = {
      expression.isInstanceOf[LessThanOrEqual]
    }

    @inline
    def isEquals: Boolean = {
      expression.isInstanceOf[EqualTo]
    }

    @inline
    def isGreater: Boolean = {
      expression.isInstanceOf[GreaterThan]
    }

    @inline
    def isGreaterEquals: Boolean = {
      expression.isInstanceOf[GreaterThanOrEqual]
    }
  }

  // it's better for reconstruction, cause `fastEquals` isn't always valid
  // for example: a < 3 do not equals 3 > a when using `fastEquals`
  // can we override `fastEquals` in Binary Node
  // like `BinaryExpression` or create new equals method in `BinaryNode`?
  @inline
  private def isSame(left: Expression, right: Expression): Boolean = {
    left.fastEquals(right)
  }

  // predicate that is "&&" or "||"
  @inline
  private def isCombinePredicate(exprs: Expression*): Boolean = {
    exprs.forall(expr => expr.isInstanceOf[CombinePredicate])
  }

  @inline
  private def isNotCombinePredicate(exprs: Expression*): Boolean = {
    exprs.forall(!_.isInstanceOf[CombinePredicate])
  }

  private def isSameCombinePredicate(head: CombinePredicate, others: CombinePredicate*) = {
    others.forall(p => p.getClass == head.getClass)
  }

  // compare left and right
  // if left and right can be compared(means left and right are the same type)
  // reture left compare right
  // else if left and right are not the same type, return none
  private def compare(left: Any, right: Any): Option[Int] = (left, right) match {
    case (leftNumber: Number, rightNumber: Number) =>
      Some(leftNumber.doubleValue().compareTo(rightNumber.doubleValue()))

    case (leftString: String, rightString: String) =>
      if (leftString == null && rightString == null) {
        Some(0)
      } else {
        Some(leftString.compareTo(rightString))
      }

    case (leftDate: Date, rightDate: Date)=>
      if (leftDate == null && rightDate == null) {
        Some(0)
      } else {
        Some(leftDate.compareTo(rightDate))
      }

    case _ =>
      None
  }
}
/**
 * Simplifies boolean expressions where the answer can be determined without evaluating both sides.
 * Note that this rule can eliminate expressions that might otherwise have been evaluated and thus
 * is only safe when evaluations of expressions does not result in side effects.
 */
object BooleanSimplification extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case and @ And(left, right) =>
        (left, right) match {
          case (Literal(true, BooleanType), r) => r
          case (l, Literal(true, BooleanType)) => l
          case (Literal(false, BooleanType), _) => Literal(false)
          case (_, Literal(false, BooleanType)) => Literal(false)
          case (_, _) => and
        }

      case or @ Or(left, right) =>
        (left, right) match {
          case (Literal(true, BooleanType), _) => Literal(true)
          case (_, Literal(true, BooleanType)) => Literal(true)
          case (Literal(false, BooleanType), r) => r
          case (l, Literal(false, BooleanType)) => l
          case (_, _) => or
        }

      case not @ Not(exp) =>
        exp match {
          case Literal(true, BooleanType) => Literal(false)
          case Literal(false, BooleanType) => Literal(true)
          case GreaterThan(l, r) => LessThanOrEqual(l, r)
          case GreaterThanOrEqual(l, r) => LessThan(l, r)
          case LessThan(l, r) => GreaterThanOrEqual(l, r)
          case LessThanOrEqual(l, r) => GreaterThan(l, r)
          case Not(e) => e
          case _ => not
        }

      // Turn "if (true) a else b" into "a", and if (false) a else b" into "b".
      case e @ If(Literal(v, _), trueValue, falseValue) => if (v == true) trueValue else falseValue
    }
  }
}

/**
 * Combines two adjacent [[Filter]] operators into one, merging the
 * conditions into one conjunctive predicate.
 */
object CombineFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ff @ Filter(fc, nf @ Filter(nc, grandChild)) => Filter(And(nc, fc), grandChild)
  }
}

/**
 * Removes filters that can be evaluated trivially.  This is done either by eliding the filter for
 * cases where it will always evaluate to `true`, or substituting a dummy empty relation when the
 * filter will always evaluate to `false`.
 */
object SimplifyFilters extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
    case Filter(Literal(null, _), child) => LocalRelation(child.output, data = Seq.empty)
    case Filter(Literal(false, BooleanType), child) => LocalRelation(child.output, data = Seq.empty)
  }
}

/**
 * Pushes [[Filter]] operators through [[Project]] operators, in-lining any [[Alias Aliases]]
 * that were defined in the projection.
 *
 * This heuristic is valid assuming the expression evaluation cost is minimal.
 */
object PushPredicateThroughProject extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, project @ Project(fields, grandChild)) =>
      val sourceAliases = fields.collect { case a @ Alias(c, _) =>
        (a.toAttribute: Attribute) -> c
      }.toMap
      project.copy(child = filter.copy(
        replaceAlias(condition, sourceAliases),
        grandChild))
  }

  def replaceAlias(condition: Expression, sourceAliases: Map[Attribute, Expression]): Expression = {
    condition transform {
      case a: AttributeReference => sourceAliases.getOrElse(a, a)
    }
  }
}

/**
 * Pushes down [[Filter]] operators where the `condition` can be
 * evaluated using only the attributes of the left or right side of a join.  Other
 * [[Filter]] conditions are moved into the `condition` of the [[Join]].
 *
 * And also Pushes down the join filter, where the `condition` can be evaluated using only the
 * attributes of the left or right side of sub query when applicable.
 *
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
 */
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Splits join condition expressions into three categories based on the attributes required
   * to evaluate them.
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    val (leftEvaluateCondition, rest) =
        condition.partition(_.references subsetOf left.outputSet)
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(_.references subsetOf right.outputSet)

    (leftEvaluateCondition, rightEvaluateCondition, commonCondition)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        split(splitConjunctivePredicates(filterCondition), left, right)

      joinType match {
        case Inner =>
          // push down the single side `where` condition into respective sides
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (commonFilterCondition ++ joinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, Inner, newJoinCond)
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case _ @ (LeftOuter | LeftSemi) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case FullOuter => f // DO Nothing for Full Outer Join
      }

    // push down the join filter into sub query scanning if applicable
    case f @ Join(left, right, joinType, joinCondition) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      joinType match {
        case Inner =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          Join(newLeft, newRight, Inner, newJoinCond)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, RightOuter, newJoinCond)
        case _ @ (LeftOuter | LeftSemi) =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond)
        case FullOuter => f
      }
  }
}

/**
 * Removes [[Cast Casts]] that are unnecessary because the input is already the correct type.
 */
object SimplifyCasts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Cast(e, dataType) if e.dataType == dataType => e
  }
}

/**
 * Combines two adjacent [[Limit]] operators into one, merging the
 * expressions into one single expression.
 */
object CombineLimits extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case ll @ Limit(le, nl @ Limit(ne, grandChild)) =>
      Limit(If(LessThan(ne, le), ne, le), grandChild)
  }
}

/**
 * Removes the inner [[CaseConversionExpression]] that are unnecessary because
 * the inner conversion is overwritten by the outer one.
 */
object SimplifyCaseConversionExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsUp {
      case Upper(Upper(child)) => Upper(child)
      case Upper(Lower(child)) => Upper(child)
      case Lower(Upper(child)) => Lower(child)
      case Lower(Lower(child)) => Lower(child)
    }
  }
}

/**
 * Speeds up aggregates on fixed-precision decimals by executing them on unscaled Long values.
 *
 * This uses the same rules for increasing the precision and scale of the output as
 * [[org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion.DecimalPrecision]].
 */
object DecimalAggregates extends Rule[LogicalPlan] {
  import Decimal.MAX_LONG_DIGITS

  /** Maximum number of decimal digits representable precisely in a Double */
  val MAX_DOUBLE_DIGITS = 15

  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
      MakeDecimal(Sum(UnscaledValue(e)), prec + 10, scale)

    case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
      Cast(
        Divide(Average(UnscaledValue(e)), Literal(math.pow(10.0, scale), DoubleType)),
        DecimalType(prec + 4, scale + 4))
  }
}
