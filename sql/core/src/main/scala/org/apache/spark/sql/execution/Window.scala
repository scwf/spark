package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, ClusteredDistribution, Partitioning}

case class Window(
    windowExpression: Seq[NamedExpression],
    windowSpec: WindowSpecDefinition,
    child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] =
    if (windowSpec.partitionSpec.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(windowSpec.partitionSpec) :: Nil
    }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    if (windowSpec.orderSpec.isEmpty && windowSpec.partitionSpec.nonEmpty) {
      windowSpec.partitionSpec.map(SortOrder(_, Ascending)) :: Nil
    } else {
      windowSpec.orderSpec :: Nil
    }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering


  def execute(): RDD[Row] = {
    null
  }
}
