package org.apache.spark.sql.execution

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, Distribution, ClusteredDistribution, Partitioning}
import org.apache.spark.util.collection.CompactBuffer

case class Window(
    projectList: Seq[Attribute],
    windowExpression: Seq[NamedExpression],
    windowSpec: WindowSpecDefinition,
    child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] =
    (projectList ++ windowExpression).map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] =
    if (windowSpec.partitionSpec.isEmpty) {
      // This operator will be very expensive.
      AllTuples :: Nil
    } else {
      ClusteredDistribution(windowSpec.partitionSpec) :: Nil
    }

  // Since window functions are adding columns to the input rows, the child's outputPartitioning
  // is preserved.
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
  private val partitionOrdering: RowOrdering =
    RowOrdering.forSchema(windowSpec.partitionSpec.map(_.dataType))
  // This is used to project expressions for the partition specification.
  @transient protected lazy val partitionGenerator =
    newProjection(windowSpec.partitionSpec, child.output)

  // rowOrdering is used to compare two rows based on the expressions in the ORDER BY clause
  // in the OVER clause. This is used for RANGE based window frame (offsets specified in a
  // RANGE frame indicates the number of groups of rows having the same ordering).
  private val rowOrdering: RowOrdering =
    RowOrdering.forSchema(windowSpec.orderSpec.map(_.dataType))
  // This is ued to project expressions for the order specification.
  @transient protected lazy val rowOrderGenerator =
    newProjection(windowSpec.orderSpec.map(_.child), child.output)

  case class ComputedWindow(
    unbound: WindowExpression,
    windowFunction: WindowFunction,
    resultAttribute: AttributeReference)

  // A list of window functions that need to be computed for each group.
  private[this] val computedWindowExpressions = windowExpression.flatMap { window =>
    window.collect {
      case w: WindowExpression =>
        ComputedWindow(
          w,
          BindReferences.bindReference(w.windowFunction, child.output),
          AttributeReference(s"windowResult:$w", w.dataType, w.nullable)())
    }
  }.toArray

  private[this] val windowFrame =
    windowSpec.frameSpecification.asInstanceOf[SpecifiedWindowFrame]

  // Create window functions.
  private[this] def windowFunctions(): Array[WindowFunction] = {
    val functions = new Array[WindowFunction](computedWindowExpressions.length)
    var i = 0
    while (i < computedWindowExpressions.length) {
      functions(i) = computedWindowExpressions(i).windowFunction.newInstance()
      functions(i).init()
      i += 1
    }
    functions
  }

  // The schema of the result of all window function evaluations
  private[this] val computedSchema = computedWindowExpressions.map(_.resultAttribute)

  private[this] val computedResultMap =
    computedWindowExpressions.map { w => w.unbound -> w.resultAttribute }.toMap

  private[this] val windowExpressionResult = windowExpression.map { window =>
    window.transform {
      case w: WindowExpression if computedResultMap.contains(w) => computedResultMap(w)
    }
  }

  def execute(): RDD[Row] = {
    child.execute().mapPartitions { iter =>
      new Iterator[Row] {

        // The position of next output row in the inputRowBuffer.
        var rowPosition: Int = 0
        // The number of buffered rows in the inputRowBuffer (the size of the current partition).
        var partitionSize: Int = 0
        // The buffer used to buffer rows in a partition.
        var inputRowBuffer: CompactBuffer[Row] = _
        // The partition key of the current partition.
        var currentPartitionKey: Row = _
        // The partition key of next partition.
        var nextPartitionKey: Row = _
        // The first row of next partition.
        var firstRowInNextPartition: Row = _
        // Indicates if this partition is the last one in the iter.
        var lastPartition: Boolean = false

        def createBoundaryEvaluator(): () => Unit = {
          def findPhysicalBoundary(
              boundary: FrameBoundary): Int = boundary match {
            case UnboundedPreceding => 0
            case UnboundedFollowing => partitionSize - 1
            case CurrentRow => rowPosition
            case ValuePreceding(value) =>
              val newPosition = rowPosition - value
              if (newPosition > 0) newPosition else 0
            case ValueFollowing(value) =>
              val newPosition = rowPosition + value
              if (newPosition < partitionSize) newPosition else partitionSize - 1
          }

          def findLogicalBoundary(
              boundary: FrameBoundary,
              searchDirection: Int,
              evaluator: Expression,
              joinedRow: JoinedRow): Int = boundary match {
            case UnboundedPreceding => 0
            case UnboundedFollowing => partitionSize - 1
            case other =>
              // CurrentRow, ValuePreceding, or ValueFollowing.
              var newPosition = rowPosition + searchDirection
              var stopSearch = false
              val currentOrderByValue = rowOrderGenerator(inputRowBuffer(rowPosition))
              while (newPosition >= 0 && newPosition < partitionSize && !stopSearch) {
                val r = rowOrderGenerator(inputRowBuffer(newPosition))
                stopSearch =
                  !(evaluator.eval(joinedRow(currentOrderByValue, r)).asInstanceOf[Boolean])
                if (!stopSearch) {
                  newPosition += searchDirection
                }
              }
              newPosition -= searchDirection

              if (newPosition < 0) {
                0
              } else if (newPosition >= partitionSize) {
                partitionSize - 1
              } else {
                newPosition
              }
          }

          windowFrame.frameType match {
            case RowFrame =>
              () => {
                frameStart = findPhysicalBoundary(windowFrame.frameStart)
                frameEnd = findPhysicalBoundary(windowFrame.frameEnd)
              }
            case RangeFrame =>
              val joinedRowForBoundaryEvaluation: JoinedRow = new JoinedRow()
              val orderByExpr = windowSpec.orderSpec.head
              val currentRowExpr =
                BoundReference(0, orderByExpr.dataType, orderByExpr.nullable)
              val examedRowExpr =
                BoundReference(1, orderByExpr.dataType, orderByExpr.nullable)
              val differenceExpr = Abs(Subtract(currentRowExpr, examedRowExpr))

              val frameStartEvaluator = windowFrame.frameStart match {
                case CurrentRow => EqualTo(currentRowExpr, examedRowExpr)
                case ValuePreceding(value) =>
                  LessThanOrEqual(differenceExpr, Cast(Literal(value), orderByExpr.dataType))
                case ValueFollowing(value) =>
                  GreaterThanOrEqual(differenceExpr, Cast(Literal(value), orderByExpr.dataType))
                case o => Literal(true) // This is just a dummy expression.
              }

              val frameEndEvaluator = windowFrame.frameEnd match {
                case CurrentRow => EqualTo(currentRowExpr, examedRowExpr)
                case ValuePreceding(value) =>
                  GreaterThanOrEqual(differenceExpr, Cast(Literal(value), orderByExpr.dataType))
                case ValueFollowing(value) =>
                  LessThanOrEqual(differenceExpr, Cast(Literal(value), orderByExpr.dataType))
                case o => Literal(true) // This is just a dummy expression.
              }

              () => {
                frameStart = findLogicalBoundary(
                  windowFrame.frameStart, -1, frameStartEvaluator, joinedRowForBoundaryEvaluation)
                frameEnd = findLogicalBoundary(
                  windowFrame.frameEnd, 1, frameEndEvaluator, joinedRowForBoundaryEvaluation)
              }
          }
        }

        val boundaryEvaluator = createBoundaryEvaluator()
        // Indicates if we the specified window frame requires us to maintain a sliding frame
        // (e.g. RANGES BETWEEN 1 PRECEDING AND CURRENT ROW) or the window frame
        // is the entire partition (e.g. ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING).
        val requireUpdateFrame: Boolean = {
          def requireUpdateBoundary(boundary: FrameBoundary): Boolean = boundary match {
            case UnboundedPreceding => false
            case UnboundedFollowing => false
            case _ => true
          }

          requireUpdateBoundary(windowFrame.frameStart) ||
            requireUpdateBoundary(windowFrame.frameEnd)
        }
        // The start position of the current frame in the partition.
        var frameStart: Int = 0
        // The end position of the current frame in the partition.
        var frameEnd: Int = -1
        // Window functions.
        val functions: Array[WindowFunction] = windowFunctions()
        // Buffers used to store input parameters for window functions. Because we may need to
        // maintain a sliding frame, we use this buffer to avoid evaluate the parameters from
        // the same row multiple times.
        val windowFunctionParameterBuffers: Array[util.LinkedList[AnyRef]] =
          functions.map(_ => new util.LinkedList[AnyRef]())

        // The projection used to generate the final result rows of this operator.
        private[this] val resultProjection =
          newProjection(
            projectList ++ windowExpressionResult,
            projectList ++ computedSchema)

        // The row used to hold results of window functions.
        private[this] val windowExpressionResultRow =
          new GenericMutableRow(computedSchema.length)

        private[this] val joinedRow = new JoinedRow6

        // Initialize this iterator.
        initialize()

        private def initialize(): Unit = {
          if (iter.hasNext) {
            val currentRow = iter.next().copy()
            nextPartitionKey = partitionGenerator(currentRow)
            firstRowInNextPartition = currentRow
            fetchNextPartition()
          } else {
            // The iter is an empty one. So, we set all of the following variables
            // to make sure hasNext will return false.
            lastPartition = true
            rowPosition = 0
            partitionSize = 0
          }
        }

        // Indicates if we will have new output row.
        override final def hasNext: Boolean = {
          !lastPartition || (rowPosition < partitionSize)
        }

        override final def next(): Row = {
          if (hasNext) {
            if (rowPosition == partitionSize) {
              // All rows of this buffer have been consumed.
              // We will move to next partition.
              fetchNextPartition()
            }
            // Get the input row for the current output row.
            val inputRow = inputRowBuffer(rowPosition)
            // Get all results of the window functions for this output row.
            var i = 0
            while (i < functions.length) {
              // println("functions(i).get(rowPosition) " + functions(i).get(rowPosition))
              windowExpressionResultRow.update(i, functions(i).get(rowPosition))
              i += 1
            }

            // Construct the output row.
            val outputRow = resultProjection(joinedRow(inputRow, windowExpressionResultRow))
            // println("outputRow " + outputRow)
            // We will move to the next one.
            rowPosition += 1
            if (requireUpdateFrame && rowPosition < partitionSize) {
              // If we need to maintain a sliding frame and
              // we will still work on this partition when next is called next time, do the update.
              updateFrame()
            }

            // Return the output row.
            outputRow
          } else {
            // no more result
            throw new NoSuchElementException
          }
        }

        // Fetch the next partition.
        private def fetchNextPartition(): Unit = {
          // Create a new buffer for input rows.
          inputRowBuffer = new CompactBuffer[Row]()
          // We already have the first row for this partition
          // (recorded in firstRowInNextPartition). Add it back.
          inputRowBuffer += firstRowInNextPartition
          // Set the current partition key.
          currentPartitionKey = nextPartitionKey
          // Now, we will start to find all rows belonging to this partition.
          // Create a variable to track if we see the next partition.
          var findNextPartition = false
          // The search will stop when we see the next partition or there is no
          // input row left in the iter.
          while (iter.hasNext && !findNextPartition) {
            // Make a copy of the input row since we will put it in the buffer.
            val currentRow = iter.next().copy()
            // Get the partition key based on the partition specification.
            val partitionKey = partitionGenerator(currentRow)
            // Check if the current row belongs the current input row.
            val comparing = partitionOrdering.compare(currentPartitionKey, partitionKey)
            if (comparing == 0) {
              // This row is still in the current partition.
              inputRowBuffer += currentRow
            } else {
              // The current input row is in a different partition.
              findNextPartition = true
              nextPartitionKey = partitionKey
              firstRowInNextPartition = currentRow
            }
          }

          // We have not seen a new partition. It means that there is no new row in the
          // iter. The current partition is the last partition of the iter.
          if (!findNextPartition) {
            lastPartition = true
          }

          // We have got all rows for the current partition.
          // Set rowPosition to 0 (the next output row will be based on the first
          // input row of this partition).
          rowPosition = 0
          // The size of this partition.
          partitionSize = inputRowBuffer.size
          // Reset all parameter buffers of window functions.
          var i = 0
          while (i < windowFunctionParameterBuffers.length) {
            windowFunctionParameterBuffers(i).clear()
            i += 1
          }
          frameStart = 0
          frameEnd = -1
          // Create the first window frame for this partition.
          // If we do not need to maintain a sliding frame, this frame will
          // have the entire partition.
          updateFrame()
        }

        // The function used to maintain the sliding frame.
        private def updateFrame(): Unit = {
          // Based on the difference between the new frame and old frame,
          // updates the buffers holding input parameters of window functions.
          // We will start to prepare input parameters starting from the row
          // indicated by offset in the input row buffer.
          def updateWindowFunctionParameterBuffers(
              numToRemove: Int,
              numToAdd: Int,
              offset: Int): Unit = {
            // First, remove unneeded entries from the head of every buffer.
            var i = 0
            while (i < numToRemove) {
              var j = 0
              while (j < windowFunctionParameterBuffers.length) {
                windowFunctionParameterBuffers(j).remove()
                j += 1
              }
              i += 1
            }
            // Then, add needed entries to the tail of every buffer.
            i = 0
            while (i < numToAdd) {
              var j = 0
              while (j < windowFunctionParameterBuffers.length) {
                // Ask the function to prepare the input parameters.
                val parameters = functions(j).prepareInputParameters(inputRowBuffer(i + offset))
                windowFunctionParameterBuffers(j).add(parameters)
                j += 1
              }
              i += 1
            }
          }

          // Record the current frame start point and end point before
          // we update them.
          val previousFrameStart = frameStart
          val previousFrameEnd = frameEnd
          boundaryEvaluator()

          //println(s"frameStart: $frameStart frameEnd: $frameEnd previousFrameStart: $previousFrameStart previousFrameEnd: $previousFrameEnd partitionSize: $partitionSize rowPosition: $rowPosition inputRowBuffer.size: ${inputRowBuffer.size}")
          updateWindowFunctionParameterBuffers(
            frameStart - previousFrameStart,
            frameEnd - previousFrameEnd,
            previousFrameEnd + 1)
          // Evaluate the current frame.
          evaluateCurrentFrame()
        }

        // Evaluate the current window frame.
        private def evaluateCurrentFrame(): Unit = {
          var i = 0
          while (i < functions.length) {
            // Reset the state of the window function.
            functions(i).reset()
            // Get all buffered input parameters based on rows of this window frame.
            val inputParameters = windowFunctionParameterBuffers(i).toArray()
            // println("inputParameters.size " + inputParameters.size)
            // Send these input parameters to the window function.
            functions(i).batchUpdate(inputParameters)
            // Ask the function to evaluate based on this window frame.
            functions(i).evaluate()
            i += 1
          }
        }
      }
    }
  }
}
