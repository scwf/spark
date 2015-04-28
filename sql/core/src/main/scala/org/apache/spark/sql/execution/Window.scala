package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, ClusteredDistribution, Partitioning}
import org.apache.spark.util.collection.CompactBuffer

case class Window(
    windowExpression: Seq[NamedExpression],
    windowSpec: WindowSpecDefinition,
    child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] =
    if (windowSpec.partitionSpec.isEmpty) {
      // This operator will be very expensive.
      AllTuples :: Nil
    } else {
      ClusteredDistribution(windowSpec.partitionSpec) :: Nil
    }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    // For now, the required child ordering has two parts.
    // The first part is the expressions in the partition specification.
    // The second part is the expressions specified in the ORDER BY cluase.
    // Basically, we first use sort to group rows based on partition specifications and then sort
    // Rows in a group based on the order specification.
    (windowSpec.partitionSpec.map(SortOrder(_, Ascending)) ++ windowSpec.orderSpec) :: Nil
  }

  // Since window functions basically add columns to input rows, this operator
  // will not change the ordering of input rows.
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering


  // Although input rows are grouped based on windowSpec.partitionSpec, we need to
  // know when we have a new partition.
  // This is to manually construct an ordering that can be used to compare rows.
  private val rowOrdering: RowOrdering =
    RowOrdering.forSchema(windowSpec.partitionSpec.map(_.dataType))
  // This is used to project expressions for partition specification.
  @transient protected lazy val partitionGenerator =
    newProjection(windowSpec.partitionSpec, child.output)

  def execute(): RDD[Row] = {
    null
    /*
    child.execute().mapPartitions { iter =>
      new Iterator[Row] {

        var position: Int = -1
        var bufferSize: Int = 0
        var currentBuffer: CompactBuffer[Row] = _
        var currentPartitionKey: Row = _
        var nextPartitionKey: Row = _
        var firstRowInNextPartition: Row = _
        var lastPartition: Boolean = false

        initialize()

        private def initialize(): Unit = {
          if (iter.hasNext) {
            val currentRow = iter.next().copy()
            nextPartitionKey = partitionGenerator(currentRow)
            firstRowInNextPartition = currentRow
            fetchNextPartition()
          } else {
            // The iter is an empty one.
            lastPartition = true
            position = 0
            bufferSize = 0
          }
        }

        override final def hasNext: Boolean = {
          !lastPartition || (position < bufferSize)
        }

        override final def next(): Row = {
          if (hasNext) {

          } else {
            // no more result
            throw new NoSuchElementException
          }
        }

        private def fetchNextPartition(): Unit = {
          currentBuffer = new CompactBuffer[Row]()
          currentBuffer += firstRowInNextPartition
          currentPartitionKey = nextPartitionKey
          var findNextPartition = false
          while (iter.hasNext && !findNextPartition) {
            val currentRow = iter.next().copy()
            val partitionKey = partitionGenerator(currentRow)
            val comparing = rowOrdering.compare(currentPartitionKey, partitionKey)
            if (comparing == 0) {
              // This row is still in the current partition.
              currentBuffer += currentRow
            } else {
              findNextPartition = true
              nextPartitionKey = partitionKey
              firstRowInNextPartition = currentRow
            }
          }

          // No new partition. It is the last partition of the iter.
          if (!findNextPartition) {
            lastPartition = true
          }

          position = 0
          bufferSize = currentBuffer.size
        }
      }



      null
    }*/
  }
}
