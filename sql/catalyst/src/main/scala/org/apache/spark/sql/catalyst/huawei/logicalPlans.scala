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

case class WindowFunction(
    partitionExpressions: Seq[Expression],
    computeExpressions: Seq[WindowAttribute],
    otherExpressions: Seq[NamedExpression],
    child: LogicalPlan) extends UnaryNode {

  override def output = (computeExpressions ++ otherExpressions).map(_.toAttribute)
}

/**
 * Logical node for "INSERT OVERWRITE [LOCAL] DIRECTORY directory
 * [ROW FORMAT row_format] STORED AS file_format SELECT ... FROM ..."
 * @param path the target path to write data.
 * @param child the child logical plan.
 * @param isLocal whether to write data to local file system.
 * @param desc describe the write property such as file format.
 */
case class WriteToDirectory[T](
    path: String,
    child: LogicalPlan,
    isLocal: Boolean,
    desc: T) extends UnaryNode {
  override def output = Seq.empty[Attribute]
}
