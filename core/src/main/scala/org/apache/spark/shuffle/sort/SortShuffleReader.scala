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

package org.apache.spark.shuffle.sort

import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.util.Comparator

import scala.collection.mutable.{ArrayBuffer, HashMap, Queue}
import scala.util.{Failure, Success, Try}

import org.apache.spark._
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{BaseShuffleHandle, FetchFailedException, ShuffleReader}
import org.apache.spark.storage._
import org.apache.spark.util.{CompletionIterator, Utils}
import org.apache.spark.util.collection.{MergeUtil, TieredDiskMerger}

/**
 * SortShuffleReader merges and aggregates shuffle data that has already been sorted within each
 * map output block.
 *
 * As blocks are fetched, we store them in memory until we fail to acquire space from the
 * ShuffleMemoryManager. When this occurs, we merge some in-memory blocks to disk and go back to
 * fetching.
 *
 * TieredDiskMerger is responsible for managing the merged on-disk blocks and for supplying an
 * iterator with their merged contents. The final iterator that is passed to user code merges this
 * on-disk iterator with the in-memory blocks that have not yet been spilled.
 */
private[spark] class SortShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext)
  extends ShuffleReader[K, C] with Logging {

  /** Manage the fetched in-memory shuffle block and related buffer */
  case class MemoryShuffleBlock(blockId: BlockId, blockData: ManagedBuffer)

  require(endPartition == startPartition + 1,
    "Sort shuffle currently only supports fetching one partition")

  private val dep = handle.dependency
  private val conf = SparkEnv.get.conf
  private val blockManager = SparkEnv.get.blockManager
  private val ser = Serializer.getSerializer(dep.serializer)
  private val shuffleMemoryManager = SparkEnv.get.shuffleMemoryManager

  private val fileBufferSize = conf.getInt("spark.shuffle.file.buffer.kb", 32) * 1024

  /** Queue to store in-memory shuffle blocks */
  private val inMemoryBlocks = new Queue[MemoryShuffleBlock]()

  /**
   * Maintain block manager and reported size of each shuffle block. The block manager is used for
   * error reporting. The reported size, which, because of size compression, may be slightly
   * different than the size of the actual fetched block, is used for calculating how many blocks
   * to spill.
   */
  private val shuffleBlockMap = new HashMap[ShuffleBlockId, (BlockManagerId, Long)]()

  /** keyComparator for mergeSort, id keyOrdering is not available,
    * using hashcode of key to compare */
  private val keyComparator: Comparator[K] = dep.keyOrdering.getOrElse(new Comparator[K] {
    override def compare(a: K, b: K) = {
      val h1 = if (a == null) 0 else a.hashCode()
      val h2 = if (b == null) 0 else b.hashCode()
      if (h1 < h2) -1 else if (h1 == h2) 0 else 1
    }
  })

  /** A merge thread to merge on-disk blocks */
  private val tieredMerger = new TieredDiskMerger(conf, dep, keyComparator, context)

  /** Shuffle block fetcher iterator */
  private var shuffleRawBlockFetcherItr: ShuffleRawBlockFetcherIterator = _

  /** Number of bytes spilled in memory and on disk */
  private var _memoryBytesSpilled: Long = 0L
  private var _diskBytesSpilled: Long = 0L

  /** Number of bytes left to fetch */
  private var unfetchedBytes: Long = 0L

  def memoryBytesSpilled: Long = _memoryBytesSpilled

  def diskBytesSpilled: Long = _diskBytesSpilled + tieredMerger.diskBytesSpilled

  override def read(): Iterator[Product2[K, C]] = {
    tieredMerger.start()

    computeShuffleBlocks()

    for ((blockId, blockOption) <- fetchShuffleBlocks()) {
      val blockData = blockOption match {
        case Success(b) => b
        case Failure(e) =>
          blockId match {
            case b @ ShuffleBlockId(shuffleId, mapId, _) =>
              val address = shuffleBlockMap(b)._1
              throw new FetchFailedException (address, shuffleId.toInt, mapId.toInt, startPartition,
                Utils.exceptionString (e))
            case _ =>
              throw new SparkException (
                s"Failed to get block $blockId, which is not a shuffle block", e)
          }
      }

      shuffleRawBlockFetcherItr.currentResult = null

      // Try to fit block in memory. If this fails, merge in-memory blocks to disk.
      val blockSize = blockData.size
      val granted = shuffleMemoryManager.tryToAcquire(blockSize)
      if (granted >= blockSize) {
        if (blockData.isDirect) {
          // If the shuffle block is allocated on a direct buffer, copy it to an on-heap buffer,
          // otherwise off heap memory will be increased to the shuffle memory size.
          val onHeapBuffer = ByteBuffer.allocate(blockSize.toInt)
          onHeapBuffer.put(blockData.nioByteBuffer)

          inMemoryBlocks += MemoryShuffleBlock(blockId, new NioManagedBuffer(onHeapBuffer))
          blockData.release()
        } else {
          inMemoryBlocks += MemoryShuffleBlock(blockId, blockData)
        }
      } else {
        logDebug(s"Granted $granted memory is not enough to store shuffle block (id: $blockId, " +
          s"size: $blockSize), spilling in-memory blocks to release the memory")

        shuffleMemoryManager.release(granted)
        spillInMemoryBlocks(MemoryShuffleBlock(blockId, blockData))
      }

      unfetchedBytes -= shuffleBlockMap(blockId.asInstanceOf[ShuffleBlockId])._2
    }

    // Make sure all the blocks have been fetched.
    assert(unfetchedBytes == 0L)

    tieredMerger.doneRegisteringOnDiskBlocks()

    // Merge on-disk blocks with in-memory blocks to directly feed to the reducer.
    val finalItrGroup = inMemoryBlocksToIterators(inMemoryBlocks) ++ Seq(tieredMerger.readMerged())
    val mergedItr =
      MergeUtil.mergeSort(finalItrGroup, keyComparator, dep.keyOrdering, dep.aggregator)

    // Update the spill metrics and do cleanup work when task is finished.
    context.taskMetrics().incMemoryBytesSpilled(memoryBytesSpilled)
    context.taskMetrics().incDiskBytesSpilled(diskBytesSpilled)

    def releaseFinalShuffleMemory(): Unit = {
      inMemoryBlocks.foreach { block =>
        block.blockData.release()
        shuffleMemoryManager.release(block.blockData.size)
      }
      inMemoryBlocks.clear()
    }
    context.addTaskCompletionListener(_ => releaseFinalShuffleMemory())

    // Release the in-memory block when iteration is completed.
    val completionItr = CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](
      mergedItr, releaseFinalShuffleMemory())

    new InterruptibleIterator(context, completionItr.map(p => (p._1, p._2)))
  }

  /**
   * Called when we've failed to acquire memory for a block we've just fetched. Figure out how many
   * blocks to spill and then spill them.
   */
  private def spillInMemoryBlocks(tippingBlock: MemoryShuffleBlock): Unit = {
    val (tmpBlockId, file) = blockManager.diskBlockManager.createTempShuffleBlock()

    // If the remaining unfetched data would fit inside our current allocation, we don't want to
    // waste time spilling blocks beyond the space needed for it.
    // Note that the number of unfetchedBytes is not exact, because of the compression used on the
    // sizes of map output blocks.
    var bytesToSpill = unfetchedBytes
    val blocksToSpill = new ArrayBuffer[MemoryShuffleBlock]()
    blocksToSpill += tippingBlock
    bytesToSpill -= tippingBlock.blockData.size
    while (bytesToSpill > 0 && !inMemoryBlocks.isEmpty) {
      val block = inMemoryBlocks.dequeue()
      blocksToSpill += block
      bytesToSpill -= block.blockData.size
    }

    _memoryBytesSpilled += blocksToSpill.map(_.blockData.size()).sum

    if (blocksToSpill.size > 1) {
      spillMultipleBlocks(file, tmpBlockId, blocksToSpill, tippingBlock)
    } else {
      spillSingleBlock(file, blocksToSpill.head)
    }

    tieredMerger.registerOnDiskBlock(tmpBlockId, file)

    logInfo(s"Merged ${blocksToSpill.size} in-memory blocks into file ${file.getName}")
  }

  private def spillSingleBlock(file: File, block: MemoryShuffleBlock): Unit = {
    val fos = new FileOutputStream(file)
    val buffer = block.blockData.nioByteBuffer()
    var channel = fos.getChannel
    var success = false

    try {
      while (buffer.hasRemaining) {
        channel.write(buffer)
      }
      success = true
    } finally {
      if (channel != null) {
        channel.close()
        channel = null
      }
      if (!success) {
        if (file.exists()) {
          file.delete()
        }
      } else {
        _diskBytesSpilled += file.length()
      }
      // When we spill a single block, it's the single tipping block that we never acquired memory
      // from the shuffle memory manager for, so we don't need to release any memory from there.
      block.blockData.release()
    }
  }

  /**
   * Merge multiple in-memory blocks to a single on-disk file.
   */
  private def spillMultipleBlocks(file: File, tmpBlockId: BlockId,
      blocksToSpill: Seq[MemoryShuffleBlock], tippingBlock: MemoryShuffleBlock): Unit = {
    val itrGroup = inMemoryBlocksToIterators(blocksToSpill)
    val partialMergedItr =
      MergeUtil.mergeSort(itrGroup, keyComparator, dep.keyOrdering, dep.aggregator)
    val curWriteMetrics = new ShuffleWriteMetrics()
    var writer = blockManager.getDiskWriter(tmpBlockId, file, ser, fileBufferSize, curWriteMetrics)
    var success = false

    try {
      partialMergedItr.foreach(writer.write)
      success = true
    } finally {
      if (!success) {
        if (writer != null) {
          writer.revertPartialWritesAndClose()
          writer = null
        }
        if (file.exists()) {
          file.delete()
        }
      } else {
        writer.commitAndClose()
        writer = null
      }
      for (block <- blocksToSpill) {
        block.blockData.release()
        if (block != tippingBlock) {
          shuffleMemoryManager.release(block.blockData.size)
        }
      }
    }
    _diskBytesSpilled += curWriteMetrics.shuffleBytesWritten
  }

  private def inMemoryBlocksToIterators(blocks: Seq[MemoryShuffleBlock])
    : Seq[Iterator[Product2[K, C]]] = {
    blocks.map{ case MemoryShuffleBlock(id, buf) =>
      blockManager.dataDeserialize(id, buf.nioByteBuffer(), ser)
        .asInstanceOf[Iterator[Product2[K, C]]]
    }
  }

  /**
   * Utility function to compute the shuffle blocks and related BlockManagerID, block size,
   * also the total request shuffle size before starting to fetch the shuffle blocks.
   */
  private def computeShuffleBlocks(): Unit = {
    val statuses = SparkEnv.get.mapOutputTracker.getServerStatuses(handle.shuffleId, startPartition)

    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]()
    for (((address, size), index) <- statuses.zipWithIndex) {
      splitsByAddress.getOrElseUpdate(address, ArrayBuffer()) += ((index, size))
    }

    splitsByAddress.foreach { case (id, blocks) =>
      blocks.foreach { case (idx, len) =>
        shuffleBlockMap.put(ShuffleBlockId(handle.shuffleId, idx, startPartition), (id, len))
        unfetchedBytes += len
      }
    }
  }

  private def fetchShuffleBlocks(): Iterator[(BlockId, Try[ManagedBuffer])] = {
    val blocksByAddress = new HashMap[BlockManagerId, ArrayBuffer[(ShuffleBlockId, Long)]]()

    shuffleBlockMap.foreach { case (block, (id, len)) =>
      blocksByAddress.getOrElseUpdate(id,
        ArrayBuffer[(ShuffleBlockId, Long)]()) += ((block, len))
    }

    shuffleRawBlockFetcherItr = new ShuffleRawBlockFetcherIterator(
      context,
      SparkEnv.get.blockManager.shuffleClient,
      blockManager,
      blocksByAddress.toSeq,
      conf.getLong("spark.reducer.maxMbInFlight", 48) * 1024 * 1024)

    val completionItr = CompletionIterator[
      (BlockId, Try[ManagedBuffer]),
      Iterator[(BlockId, Try[ManagedBuffer])]](shuffleRawBlockFetcherItr,
        context.taskMetrics.updateShuffleReadMetrics())

    new InterruptibleIterator[(BlockId, Try[ManagedBuffer])](context, completionItr)
  }
}
