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

package org.apache.spark.sql.hbase

import java.io.{ObjectInputStream, ObjectOutputStream, IOException}
import java.lang.reflect.Array
import scala.Array
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkEnv
import org.apache.spark.Partitioner
import org.apache.spark.util.{Utils, CollectionsUtils}
import org.apache.spark.serializer.JavaSerializer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.HTable

class HBasePartitioner [K : Ordering : ClassTag, V](
     @transient partitions: Int,
     @transient rdd: RDD[_ <: Product2[K,V]],
     conf: Configuration)(splitKeys: Array[K])
  extends Partitioner {

  // We allow partitions = 0, which happens when sorting an empty RDD under the default settings.
  require(partitions >= 0, s"Number of partitions cannot be negative but found $partitions.")

  private var ordering = implicitly[Ordering[K]]

  private var rangeBounds: Array[K] = splitKeys

  def numPartitions = rangeBounds.length + 1

  private var binarySearch: ((Array[K], K) => Int) = CollectionsUtils.makeBinarySearch[K]

  // todo: test this
  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[K]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
    partition
  }

  override def equals(other: Any): Boolean = other match {
    case r: HBasePartitioner[_,_] =>
      r.rangeBounds.sameElements(rangeBounds) && r.ascending == ascending
    case _ =>
      false
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    var i = 0
    while (i < rangeBounds.length) {
      result = prime * result + rangeBounds(i).hashCode
      i += 1
    }
    result = prime * result + ascending.hashCode
    result
  }

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream) {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => out.defaultWriteObject()
      case _ =>
        out.writeBoolean(ascending)
        out.writeObject(ordering)
        out.writeObject(binarySearch)

        val ser = sfactory.newInstance()
        Utils.serializeViaNestedStream(out, ser) { stream =>
          stream.writeObject(scala.reflect.classTag[Array[K]])
          stream.writeObject(rangeBounds)
        }
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream) {
    val sfactory = SparkEnv.get.serializer
    sfactory match {
      case js: JavaSerializer => in.defaultReadObject()
      case _ =>
        ascending = in.readBoolean()
        ordering = in.readObject().asInstanceOf[Ordering[K]]
        binarySearch = in.readObject().asInstanceOf[(Array[K], K) => Int]

        val ser = sfactory.newInstance()
        Utils.deserializeViaNestedStream(in, ser) { ds =>
          implicit val classTag = ds.readObject[ClassTag[Array[K]]]()
          rangeBounds = ds.readObject[Array[K]]()
        }
    }
  }
}

//Todo： test this
object HBasePartitioner {
  implicit def orderingRowKey[A <: ImmutableBytesWritable]: Ordering[A] = OrderingRowKey

  /**
   * Return the start keys of all of the regions in this table,
   * as a list of ImmutableBytesWritable.
   * Todo: should move to hbase relation after code clean
   */
  def getRegionStartKeys(table: HTable): List[ImmutableBytesWritable] = {
    val byteKeys: Array[Array[Byte]] = table.getStartKeys
    val ret = ArrayBuffer[ImmutableBytesWritable](byteKeys.length)
    for (byteKey <- byteKeys) {
      ret += new ImmutableBytesWritable(byteKey)
    }
    ret
  }
}

object OrderingRowKey extends Ordering[ImmutableBytesWritable] {
  def compare(a: ImmutableBytesWritable, b: ImmutableBytesWritable) = a.compareTo(b)
}
