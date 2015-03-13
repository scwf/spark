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

package org.apache.spark.sql.catalyst.huawei

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

/**
 * translate WindowAttribute in SelectExpression to attribute
 */
object WindowAttributes extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: WindowFunction =>
      q transformExpressions {
        case u @ WindowAttribute(child: AggregateExpression, name, windowSpec) =>
          // set window spec in AggregateExpression for execution and GlobalAggregates check
          child.windowSpec = windowSpec
          u
      }
    case q: LogicalPlan =>
      q transformExpressions {
        // translate WindowAttribute in SelectExpression to attribute
        case u @ WindowAttribute(children, name, windowSpec) => u.toAttribute
      }
  }
}
