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

package org.apache.spark.sql.execution

import java.util.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.SQLContext

/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where `groupingExpressions` are equal are present.
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
@DeveloperApi
case class Aggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  // sort可能会按照 aggregateExpressions 里面没有的列 但在groupingExpressions 有的列来排序，
  // 当前的做法是 修改 aggregate 的aggregateExpressions 然后在sort上方添加 project进行修剪
  // 这种case 是否可以去掉project？ 毕竟 project 会多走一遍， 可能会打破 物理row 和 逻辑output 不匹配

  override def requiredChildDistribution: List[Distribution] = {
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) { // 如果没有group by，則对所有记录聚合
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }
  }
  // 注意对于语句 select a/sum(b) from testData2 group by a
  // 1. 该语句会生成出两geaggregate 第一个partitial的，其中
  // groupingexpression是 attributereference a
  // 但 aggexpressions 居然长度是 2, a 和 sum（b）
  // a * sum(b) 也类似
  // 2 第二是全局的 aggregate ，这个 里面的aggexpression就正常了，长度为1,是devide/multiply

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute) // toAttribute sum as s =》s

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
   * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(
      unbound: AggregateExpression,
      aggregate: AggregateExpression,
      resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, child.output), // 这么写 在做全局聚合时 就可以拿到局部聚合的结果了，不用再算一遍
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)()) // 给聚合计算结果 新命名为 aggResult的 AttributeReference，主要是生成resultExpressions使用
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  private[this] def newAggregateBuffer(): Array[AggregateFunction] = {
    val buffer = new Array[AggregateFunction](computedAggregates.length)
    var i = 0
    while (i < computedAggregates.length) {
      buffer(i) = computedAggregates(i).aggregate.newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  private[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }
  // 将aggregateExpressions中的expression替换为 最终结果 attribute， 便于使用中间聚合结果（指的是该聚合算子的计算的中间聚合结果，这里就是a 和 sum的row）的输出，
  // 比如 (CAST(a#6, LongType) * CombineSum(PartialSum#46L)) AS c0#41L =》
  // (CAST(a#6, LongType) * aggResult:CombineSum(PartialSum#46L)#49L) AS c0#41L

  override def execute(): RDD[Row] = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow) // 更新聚合值
            i += 1
          }
        }
        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length) // 如果groupingExpressions为空，最后结果是一行

        var i = 0
        while (i < buffer.length) {
          aggregateResults(i) = buffer(i).eval(EmptyRow) // 获取聚合值
          i += 1
        }
        // aggregateResults 是中间计算结果，最后的计算结果要根据他再生成

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new HashMap[Row, Array[AggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[Row] {
          private[this] val hashTableIter = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2)) // namedGroups 不是多余的，之所以上面没有，是因为上面groupingExpressions为空所以namedGroups也为空
          private[this] val joinedRow = new JoinedRow4

          override final def hasNext: Boolean = hashTableIter.hasNext

          override final def next(): Row = {
            val currentEntry = hashTableIter.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = 0
            while (i < currentBuffer.length) {
              // Evaluating an aggregate buffer returns the result.  No row is required since we
              // already added all rows in the group using update.
              aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup)) // 这个joinedrow 保证了 其schema和 computedSchema ++ namedGroups.map(_._2)对应
          }
        }
      }
    }
  }
}
