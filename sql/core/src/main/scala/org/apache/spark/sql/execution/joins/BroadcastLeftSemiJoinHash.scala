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

package org.apache.spark.sql.execution.joins

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}

/**
 * :: DeveloperApi ::
 * Build the right table's join keys into a HashSet, and iteratively go through the left
 * table, to find the if join keys are in the Hash set.
 */
@DeveloperApi
case class BroadcastLeftSemiJoinHash(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BinaryNode with HashJoin {

  override val buildSide: BuildSide = BuildRight

  override def output: Seq[Attribute] = left.output

  @transient private lazy val boundCondition =
    InterpretedPredicate.create(
      condition
        .map(c => BindReferences.bindReference(c, left.output ++ right.output))
        .getOrElse(Literal(true)))

  override def execute(): RDD[Row] = {
    val buildIter= buildPlan.execute().map(_.copy()).collect().toIterator
    val hashMap = new java.util.HashMap[Row, scala.collection.mutable.Set[Row]]()
    var currentRow: Row = null

    // Create a Hash set of buildKeys
    while (buildIter.hasNext) {
      currentRow = buildIter.next()
      val rowKey = buildSideKeyGenerator(currentRow)
      if (!rowKey.anyNull) {
        if (!hashMap.containsKey(rowKey)) {
          val rowSet = scala.collection.mutable.Set[Row]()
          rowSet.add(currentRow.copy())
          hashMap.put(rowKey, rowSet)
        } else {
          hashMap.get(rowKey).add(currentRow.copy())
        }
      }
    }

    val broadcastedRelation = sparkContext.broadcast(hashMap)

    streamedPlan.execute().mapPartitions { streamIter =>
      val joinKeys = streamSideKeyGenerator()
      val joinedRow = new JoinedRow
      streamIter.filter(current => {
        !joinKeys(current).anyNull &&
          broadcastedRelation.value.containsKey(joinKeys.currentValue) &&
          broadcastedRelation.value.get(joinKeys.currentValue).exists {
            build: Row => boundCondition(joinedRow(current, build))
          }
      })
    }
  }
}
