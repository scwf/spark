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

import org.apache.spark.sql.catalyst.analysis.EliminateAnalysisOperators
import org.apache.spark.sql.catalyst.expressions.{Literal, Expression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._

class FilterSimplificationSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateAnalysisOperators) ::
      Batch("Constant Folding", FixedPoint(10),
        NullPropagation,
        ConstantFolding,
        ConditionSimplification,
        BooleanSimplification,
        SimplifyFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int)
  
  def doFavor(originCondition: Expression, optimizedCondition: Expression): Unit = {
    val originQuery = testRelation.where(originCondition).analyze
    val optimized = Optimize(originQuery)
    val expected = testRelation.where(optimizedCondition).analyze
    comparePlans(optimized, expected)
  }

  def doFavor(originCondition: Expression): Unit = {
    val originQuery = testRelation.where(originCondition).analyze
    val optimized = Optimize(originQuery)
    val expected = testRelation
    comparePlans(optimized, expected)
  }

  test("literal in front of attribute") {
    doFavor(Literal(1) < 'a || Literal(2) < 'a, 'a > 1)
  }

  test("combine the same filter condition") {
    doFavor('a < 1 || 'a < 1, 'a < 1)
    doFavor('a > 2 && 'a > 2, 'a > 2)
    doFavor(('a < 1 && 'a < 2) || ('a < 1 && 'a < 2), 'a < 1)
  }

  test("combine literal binary comparison") {
    doFavor('a === 1 && 'a < 1)
    doFavor('a === 1 || 'a < 1, 'a <= 1)

    doFavor('a === 1 && 'a === 2)
    doFavor('a === 1 || 'a === 2, 'a === 1 || 'a === 2)

    doFavor('a <= 1 && 'a > 1)
    doFavor('a <= 1 || 'a > 1)

    doFavor('a < 1 && 'a >= 1)
    doFavor('a < 1 || 'a >= 1)

    doFavor('a > 3 && 'a > 2, 'a > 3)
    doFavor('a > 3 || 'a > 2, 'a > 2)

  }

  test("combine predicate : 2 same combine") {
    doFavor('a < 1 || 'b > 2 || 'a >= 1)
    doFavor('a < 1 && 'b > 2 && 'a >= 1)

    doFavor('a < 2 || 'b > 3 || 'b > 2, 'a < 2 || 'b > 2)
    doFavor('a < 2 && 'b > 3 && 'b > 2, 'a < 2 && 'b > 3)

    doFavor('a < 2 || ('b > 3 || 'b > 2), 'b > 2 || 'a < 2)
    doFavor('a < 2 && ('b > 3 && 'b > 2), 'b > 3 && 'a < 2)

    doFavor('a < 2 || 'a === 3 || 'a > 5, 'a < 2 || 'a === 3 || 'a > 5)
  }

  test("combine predicate : 2 difference combine") {
    doFavor(('a < 2 || 'a > 3) && 'a > 4, 'a > 4)
    doFavor(('a < 2 || 'b > 3) && 'a < 2, 'a < 2)

    doFavor('a < 2 || ('a >= 2 && 'b > 1), 'b > 1 ||  'a < 2)
    doFavor('a < 2 || ('a === 2 && 'b > 1), 'a < 2 || ('a === 2 && 'b > 1))

    doFavor('a > 3 || ('a > 2 && 'a < 4), 'a > 2)
  }

  test("multi left, single right") {
    doFavor(('a < 2 || 'a > 3 || 'b > 5) && 'a < 2, 'a < 2)
  }

  test("multi left, multi right") {
    doFavor(('a < 2 || 'b > 3) && ('a < 2 || 'c > 5), 'a < 2 || ('b > 3 && 'c > 5))

    var input = ('a === 'b || 'b > 3) && ('a === 'b || 'a > 3) && ('a === 'b || 'a < 5)
    var expected = 'a === 'b || ('b > 3 && 'a > 3 && 'a < 5)
    doFavor(input, expected)

    input = ('a === 'b || 'b > 3) && ('a === 'b || 'a > 3) && ('a === 'b || 'a > 1)
    expected = 'a === 'b || ('b > 3 && 'a > 3)
    doFavor(input, expected)
  }

}
