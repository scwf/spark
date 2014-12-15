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

abstract class Optimizer extends RuleExecutor[LogicalPlan]

object DefaultOptimizer extends Optimizer {
  val batches =
    Batch("Combine Limits", FixedPoint(100),
      CombineLimits) ::
    Batch("ConstantFolding", FixedPoint(100),
      NullPropagation,
      ConstantFolding,
      LikeSimplification,
      SimplifyFilters,
      BooleanSimplification,
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
  import BinaryPredicate.CombinePredicate
  import BinaryComparison.LiteralComparison

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
    case Filter(Literal(null, _), child) => LocalRelation(child.output, data = Seq.empty)
    case Filter(Literal(false, BooleanType), child) => LocalRelation(child.output, data = Seq.empty)
    // simplify Or or And predicate
    case Filter(bp @ CombinePredicate(_, _), child) =>
      Filter(optimizePredicate(bp), child)
  }
  // TODO : check if support EqualsNullSafe well
  def optimizePredicate(expr: BinaryPredicate): Expression = expr match {
    // predicate is || or &&, left equals right , drop right, keep left
    // if left is also || or &&, optimize it
    case CombinePredicate(left, right) if left.fastEquals(right) =>
      if (left.isInstanceOf[BinaryPredicate]) {
        optimizePredicate(left.asInstanceOf[BinaryPredicate])
      } else {
        left
      }

    // left and right are both binary comparison that contains literal
    // here LiteralComparison unapply method changed the child position
    // if there is no combination, should return the origin one
    case origin @ CombinePredicate(LiteralComparison(left), LiteralComparison(right)) =>
      val changed = CombinePredicate(left, right, isOR(origin))
      val optimized = combineComparison(left , right, isOR(origin))
      if (changed.fastEquals(optimized)) {
        origin
      } else {
        optimized
      }

    case origin @ BinaryPredicate(left @ BinaryPredicate(left1, right1), right)
      if !isCombinePredicate(left1, right1, right) && isCombinePredicate(origin, left) =>
      val optimizedLeft = optimizePredicate(left)
      if (left.fastEquals(optimizedLeft)) {
        if (left1.fastEquals(right) || right1.fastEquals(right)) {
          if (isSameCombinePredicate(origin, left)) optimizedLeft else right
        } else {
          val left1Right = CombinePredicate(left1, right, isOR(origin))
          val right1Right = CombinePredicate(right1, right, isOR(origin))
          val optimizedLR = optimizePredicate(left1Right)
          val optimizedRR = optimizePredicate(right1Right)
          if (left1Right.fastEquals(optimizedLR) && right1Right.fastEquals(optimizedRR)) {
            origin
          } else if ((!isCombinePredicate(optimizedLR) && !isCombinePredicate(optimizedRR))
            || isSameCombinePredicate(origin, left)) {
            optimizePredicate(CombinePredicate(optimizedLR, optimizedRR, isOR(left)))
          } else if (optimizedLR.isInstanceOf[Literal] || optimizedRR.isInstanceOf[Literal]) {
            CombinePredicate(optimizedLR, optimizedRR, isOR(left))
          } else {
            origin
          }
        }
      } else if (!isCombinePredicate(optimizedLeft)) {
        optimizePredicate(CombinePredicate(optimizedLeft, right, isOR(origin)))
      } else {
        origin
      }

    case origin @ BinaryPredicate(left, right @ BinaryPredicate(left2, right2))
      if !isCombinePredicate(left2, right2, right) && isCombinePredicate(origin, right) =>
      val changed = CombinePredicate(right, left, isOR(origin))
      val optimized = optimizePredicate(changed)
      if (changed.fastEquals(optimized)) {
        origin
      } else {
        optimized
      }

    case origin @ BinaryPredicate(left @ BinaryPredicate(ll, lr), right @ BinaryPredicate(rl, rr))
      if !isCombinePredicate(ll, lr, rl, rr)
        && isCombinePredicate(origin, left, right)
        && isSameCombinePredicate(left, right) =>
      val optimizedLeft = optimizePredicate(left)
      val optimizedRight = optimizePredicate(right)

      if (left.fastEquals(optimizedLeft)) {
        if (right.fastEquals(optimizedRight)) {
          val llrl = CombinePredicate(ll, rl, isOR(left))
          val lrrl = CombinePredicate(lr, rl, isOR(left))
          val llrr = CombinePredicate(ll, rr, isOR(left))
          val lrrr = CombinePredicate(lr, rr, isOR(left))
        }
      }
      null

    // TODO: 添加处理其他可被优化的处理过程
    case other =>
      other
  }

  private def isOR(predicate: Predicate): Boolean = {
    predicate.isInstanceOf[Or]
  }

  // predicate that is "&&" or "||"
  private def isCombinePredicate(exprs: Expression*): Boolean = {
    exprs.foreach(expr => if (!expr.isInstanceOf[Or] && !expr.isInstanceOf[And]) return false)
    true
  }

  private def isSameCombinePredicate(predicates: Predicate*): Boolean = {
    val head = predicates.head
    if (head.isInstanceOf[Or] || head.isInstanceOf[And]) {
      predicates.tail.forall(p => p.getClass == head.getClass)
    } else {
      false
    }
  }

  private def isLess(comparison: BinaryComparison): Boolean = {
    comparison.isInstanceOf[LessThan]
  }

  private def isLessEquals(comparison: BinaryComparison): Boolean = {
    comparison.isInstanceOf[LessThanOrEqual]
  }

  private def isEquals(comparison: BinaryComparison): Boolean = {
    comparison.isInstanceOf[EqualTo]
  }

  private def isGreater(comparison: BinaryComparison): Boolean = {
    comparison.isInstanceOf[GreaterThan]
  }

  private def isGreaterEquals(comparison: BinaryComparison): Boolean = {
    comparison.isInstanceOf[GreaterThanOrEqual]
  }

  def combineComparison(
    left: BinaryComparison,
    right: BinaryComparison,
    isOr: Boolean = true): Expression = {
    val origin = if (isOr) {
      Or(left, right)
    } else {
      And(left, right)
    }
    // if not the same attribute, do nothing
    if (!left.left.fastEquals(right.left)) {
      origin
    } else {
      (left, right) match {
        case (BinaryComparison(attr, Literal(vLeft, tLeft @ NativeType())),
              BinaryComparison(_, Literal(vRight, tRight @ NativeType()))) =>
          val result = compare(vLeft, vRight)
          result.filter(_ > 0).map(c => {
            if (((isLess(left) || isLessEquals(left))
              && (isLess(right) || isLessEquals(right) || isEquals(right)))) {
              if (isOr) left else right
            } else if ((isLess(left) || isLessEquals(left))
              && (isGreater(right) || isGreaterEquals(right))) {
              if (isOr) Literal(true, BooleanType) else origin
            } else if ((isEquals(left) || isGreater(left) || isGreaterEquals(left))
              && (isLess(right) || isLessEquals(right) || isEquals(right))) {
              if (isOr) origin else Literal(false, BooleanType)
            } else if (isEquals(left) && (isGreater(right) || isGreaterEquals(right))) {
              if (isOr) right else left
            } else if ((isGreater(left) || isGreaterEquals(left))
              && (isLess(right) || isEquals(right) || isLessEquals(right))) {
              if (isOr) origin else Literal(false, BooleanType)
            } else {
              if (isOr) right else left
            }
          }).getOrElse(result.filter(_ == 0).map(c => {
              if (isOr) {
                if ((left.symbol == right.symbol)
                  || (isLessEquals(left) && (isLess(right) || isEquals(right)))
                  || (isGreaterEquals(left) && (isGreater(right) || isEquals(right)))){
                  left
                } else if(((isEquals(left) || isLess(left)) && isLessEquals(right))
                  || ((isEquals(left) || isGreater(left)) && isGreaterEquals(right))) {
                  right
                } else if ((isEquals(left) && isLess(right))
                  || (isEquals(right) && isLess(left))) {
                  LessThanOrEqual(attr, Literal(vLeft, tLeft))
                } else if ((isEquals(left) && isGreater(right))
                  || (isEquals(right) && isGreater(left))){
                  GreaterThanOrEqual(attr, Literal(vLeft, tLeft))
                } else if((isLess(left) && isGreater(right))
                  || (isGreater(left) && isLess(right))) {
                  Not(EqualTo(attr, Literal(vLeft, tRight)))
                }else {
                  Literal(true, BooleanType)
                }
              } else {
                if ((left.symbol == right.symbol)
                  || (isLess(left) && isLessEquals(right))
                  || (isGreater(left) && isGreaterEquals(right))) {
                  left
                } else if((isEquals(left) && (isLessEquals(right) || isGreaterEquals(right)))
                  || (isEquals(right) && (isLessEquals(left) || isGreaterEquals(left)))) {
                  EqualTo(attr, Literal(vLeft, tLeft))
                } else if ((isLessEquals(left) && isLess(right))
                  || (isGreaterEquals(left) && isGreater(right))) {
                  right
                } else {
                  Literal(false, BooleanType)
                }
              }
          }).getOrElse(result.filter(_ < 0).map(c => {
            // if there is no combination, return the origin one
            // since `fastEqual` is not equals when `A.left = B.right && A.right = B.left`
            val changed = CombinePredicate(right, left, isOr)
            val optimized = combineComparison(right, left, isOr)
            if (changed.fastEquals(optimized)) {
              origin
            } else {
              optimized
            }
          }).getOrElse(origin)))

        case _ =>
          origin
      }
    }
  }

  private def compare(left: Any, right: Any): Option[Int] = {
    left match {
      case numLeft: Number if right.isInstanceOf[Number] =>
        val numRight = right.asInstanceOf[Number]
        Some(numLeft.doubleValue().compareTo(numRight.doubleValue()))

      case strLeft: String if right.isInstanceOf[String] =>
        val strRight = right.toString
        if (strLeft == null && strRight == null) {
          Some(0)
        } else {
          Some(strLeft.compareTo(strRight))
        }

      case dateLeft: Date if right.isInstanceOf[Date]=>
        val dateRight = right.asInstanceOf[Date]
        if (dateLeft == null && dateLeft == dateRight) {
          Some(0)
        } else {
          Some(dateLeft.compareTo(dateRight))
        }

      case _ =>
        None
    }
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
