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

import org.apache.hadoop.hive.ql.plan.PlanUtils
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.ql.parse.{ASTNode, SemanticAnalyzer}

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.hive.{HiveContext, HiveInspectors}
import org.apache.spark.sql.catalyst.huawei.WriteToDirectory
import org.apache.hadoop.hive.conf.HiveConf


case class WriteToDirs(hiveconf: HiveConf) extends Rule[LogicalPlan] with HiveInspectors {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Wait until children are resolved.
    case p: LogicalPlan if !p.childrenResolved => p

    case WriteToDirectory(path, child, isLocal, extra: ASTNode) =>
      // use hive analyzer to analyze row format info and file format info
      val sa = new SemanticAnalyzer(hiveconf) {
        override def analyzeInternal(ast: ASTNode) {
          val ctx_1 = this.initPhase1Ctx()
          this.doPhase1(ast, this.getQB, ctx_1)
          this.getMetaData(this.getQB)
        }
      }
      // analyze row format info
      sa.analyze(extra, new Context(hiveconf))
      val localDesc = sa.getQB.getLLocalDirectoryDesc

      // generate column name and related type info as hive style
      val Array(cols, types) = child.output.foldLeft(Array("", ""))((r, a) => {
        r(0) = r(0) + a.name + ","
        r(1) = r(1) + a.dataType.toTypeInfo.getTypeName + ":"
        r
      })

      val tableDesc =
        PlanUtils.getDefaultTableDesc(localDesc, cols.dropRight(1), types.dropRight(1))

      WriteToDirectory(path, child, isLocal, tableDesc)
  }
}
