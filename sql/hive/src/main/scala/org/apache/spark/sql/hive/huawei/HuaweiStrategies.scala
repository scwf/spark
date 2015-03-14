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

package org.apache.spark.sql.hive.huawei

import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.huawei
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.HiveContext


private[sql] trait HuaweiStrategies {
  // Possibly being too clever with types here... or not clever enough.
  self: SQLContext#SparkPlanner =>

  val hiveContext: HiveContext

  object HuaweiStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case huawei.WindowFunction(partition, compute, other, child) =>
        execution.WindowFunction(partition, compute, other, planLater(child)) :: Nil
      case huawei.WriteToDirectory(path, child, isLocal, desc: TableDesc) =>
        execution.WriteToDirectory(path, planLater(child), isLocal, desc) :: Nil
      case _ => Nil
    }
  }

}
