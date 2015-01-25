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

package org.apache.spark.ml.recommendation

import java.{util => ju}

import scala.collection.mutable

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.github.fommil.netlib.LAPACK.{getInstance => lapack}
import org.netlib.util.intW

import org.apache.spark.{HashPartitioner, Logging, Partitioner}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.types.{DoubleType, FloatType, IntegerType, StructField, StructType}
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.{OpenHashMap, OpenHashSet, SortDataFormat, Sorter}
import org.apache.spark.util.random.XORShiftRandom

/**
 * Common params for ALS.
 */
private[recommendation] trait ALSParams extends Params with HasMaxIter with HasRegParam
  with HasPredictionCol {

  /** Param for rank of the matrix factorization. */
  val rank = new IntParam(this, "rank", "rank of the factorization", Some(10))
  def getRank: Int = get(rank)

  /** Param for number of user blocks. */
  val numUserBlocks = new IntParam(this, "numUserBlocks", "number of user blocks", Some(10))
  def getNumUserBlocks: Int = get(numUserBlocks)

  /** Param for number of item blocks. */
  val numItemBlocks =
    new IntParam(this, "numItemBlocks", "number of item blocks", Some(10))
  def getNumItemBlocks: Int = get(numItemBlocks)

  /** Param to decide whether to use implicit preference. */
  val implicitPrefs =
    new BooleanParam(this, "implicitPrefs", "whether to use implicit preference", Some(false))
  def getImplicitPrefs: Boolean = get(implicitPrefs)

  /** Param for the alpha parameter in the implicit preference formulation. */
  val alpha = new DoubleParam(this, "alpha", "alpha for implicit preference", Some(1.0))
  def getAlpha: Double = get(alpha)

  /** Param for the column name for user ids. */
  val userCol = new Param[String](this, "userCol", "column name for user ids", Some("user"))
  def getUserCol: String = get(userCol)

  /** Param for the column name for item ids. */
  val itemCol =
    new Param[String](this, "itemCol", "column name for item ids", Some("item"))
  def getItemCol: String = get(itemCol)

  /** Param for the column name for ratings. */
  val ratingCol = new Param[String](this, "ratingCol", "column name for ratings", Some("rating"))
  def getRatingCol: String = get(ratingCol)

  /**
   * Validates and transforms the input schema.
   * @param schema input schema
   * @param paramMap extra params
   * @return output schema
   */
  protected def validateAndTransformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    assert(schema(map(userCol)).dataType == IntegerType)
    assert(schema(map(itemCol)).dataType== IntegerType)
    val ratingType = schema(map(ratingCol)).dataType
    assert(ratingType == FloatType || ratingType == DoubleType)
    val predictionColName = map(predictionCol)
    assert(!schema.fieldNames.contains(predictionColName),
      s"Prediction column $predictionColName already exists.")
    val newFields = schema.fields :+ StructField(map(predictionCol), FloatType, nullable = false)
    StructType(newFields)
  }
}

/**
 * Model fitted by ALS.
 */
class ALSModel private[ml] (
    override val parent: ALS,
    override val fittingParamMap: ParamMap,
    k: Int,
    userFactors: RDD[(Int, Array[Float])],
    itemFactors: RDD[(Int, Array[Float])])
  extends Model[ALSModel] with ALSParams {

  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = {
    import dataset.sqlContext._
    import org.apache.spark.ml.recommendation.ALSModel.Factor
    val map = this.paramMap ++ paramMap
    // TODO: Add DSL to simplify the code here.
    val instanceTable = s"instance_$uid"
    val userTable = s"user_$uid"
    val itemTable = s"item_$uid"
    val instances = dataset.as(Symbol(instanceTable))
    val users = userFactors.map { case (id, features) =>
      Factor(id, features)
    }.as(Symbol(userTable))
    val items = itemFactors.map { case (id, features) =>
      Factor(id, features)
    }.as(Symbol(itemTable))
    val predict: (Seq[Float], Seq[Float]) => Float = (userFeatures, itemFeatures) => {
      if (userFeatures != null && itemFeatures != null) {
        blas.sdot(k, userFeatures.toArray, 1, itemFeatures.toArray, 1)
      } else {
        Float.NaN
      }
    }
    val inputColumns = dataset.schema.fieldNames
    val prediction =
      predict.call(s"$userTable.features".attr, s"$itemTable.features".attr) as map(predictionCol)
    val outputColumns = inputColumns.map(f => s"$instanceTable.$f".attr as f) :+ prediction
    instances
      .join(users, LeftOuter, Some(map(userCol).attr === s"$userTable.id".attr))
      .join(items, LeftOuter, Some(map(itemCol).attr === s"$itemTable.id".attr))
      .select(outputColumns: _*)
  }

  override private[ml] def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap)
  }
}

private object ALSModel {
  /** Case class to convert factors to SchemaRDDs */
  private case class Factor(id: Int, features: Seq[Float])
}

/**
 * Alternating Least Squares (ALS) matrix factorization.
 *
 * ALS attempts to estimate the ratings matrix `R` as the product of two lower-rank matrices,
 * `X` and `Y`, i.e. `X * Yt = R`. Typically these approximations are called 'factor' matrices.
 * The general approach is iterative. During each iteration, one of the factor matrices is held
 * constant, while the other is solved for using least squares. The newly-solved factor matrix is
 * then held constant while solving for the other factor matrix.
 *
 * This is a blocked implementation of the ALS factorization algorithm that groups the two sets
 * of factors (referred to as "users" and "products") into blocks and reduces communication by only
 * sending one copy of each user vector to each product block on each iteration, and only for the
 * product blocks that need that user's feature vector. This is achieved by pre-computing some
 * information about the ratings matrix to determine the "out-links" of each user (which blocks of
 * products it will contribute to) and "in-link" information for each product (which of the feature
 * vectors it receives from each user block it will depend on). This allows us to send only an
 * array of feature vectors between each user block and product block, and have the product block
 * find the users' ratings and update the products based on these messages.
 *
 * For implicit preference data, the algorithm used is based on
 * "Collaborative Filtering for Implicit Feedback Datasets", available at
 * [[http://dx.doi.org/10.1109/ICDM.2008.22]], adapted for the blocked approach used here.
 *
 * Essentially instead of finding the low-rank approximations to the rating matrix `R`,
 * this finds the approximations for a preference matrix `P` where the elements of `P` are 1 if
 * r > 0 and 0 if r <= 0. The ratings then act as 'confidence' values related to strength of
 * indicated user
 * preferences rather than explicit ratings given to items.
 */
class ALS extends Estimator[ALSModel] with ALSParams {

  import org.apache.spark.ml.recommendation.ALS.Rating

  def setRank(value: Int): this.type = set(rank, value)
  def setNumUserBlocks(value: Int): this.type = set(numUserBlocks, value)
  def setNumItemBlocks(value: Int): this.type = set(numItemBlocks, value)
  def setImplicitPrefs(value: Boolean): this.type = set(implicitPrefs, value)
  def setAlpha(value: Double): this.type = set(alpha, value)
  def setUserCol(value: String): this.type = set(userCol, value)
  def setItemCol(value: String): this.type = set(itemCol, value)
  def setRatingCol(value: String): this.type = set(ratingCol, value)
  def setPredictionCol(value: String): this.type = set(predictionCol, value)
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  def setRegParam(value: Double): this.type = set(regParam, value)

  /** Sets both numUserBlocks and numItemBlocks to the specific value. */
  def setNumBlocks(value: Int): this.type = {
    setNumUserBlocks(value)
    setNumItemBlocks(value)
    this
  }

  setMaxIter(20)
  setRegParam(1.0)

  override def fit(dataset: SchemaRDD, paramMap: ParamMap): ALSModel = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val ratings =
      dataset.select(map(userCol).attr, map(itemCol).attr, Cast(map(ratingCol).attr, FloatType))
        .map { row =>
          new Rating(row.getInt(0), row.getInt(1), row.getFloat(2))
        }
    val (userFactors, itemFactors) = ALS.train(ratings, rank = map(rank),
      numUserBlocks = map(numUserBlocks), numItemBlocks = map(numItemBlocks),
      maxIter = map(maxIter), regParam = map(regParam), implicitPrefs = map(implicitPrefs),
      alpha = map(alpha))
    val model = new ALSModel(this, map, map(rank), userFactors, itemFactors)
    Params.inheritValues(map, this, model)
    model
  }

  override private[ml] def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap)
  }
}

private[recommendation] object ALS extends Logging {

  /** Rating class for better code readability. */
  private[recommendation] case class Rating(user: Int, item: Int, rating: Float)

  /** Cholesky solver for least square problems. */
  private[recommendation] class CholeskySolver {

    private val upper = "U"
    private val info = new intW(0)

    /**
     * Solves a least squares problem with L2 regularization:
     *
     *   min norm(A x - b)^2^ + lambda * n * norm(x)^2^
     *
     * @param ne a [[NormalEquation]] instance that contains AtA, Atb, and n (number of instances)
     * @param lambda regularization constant, which will be scaled by n
     * @return the solution x
     */
    def solve(ne: NormalEquation, lambda: Double): Array[Float] = {
      val k = ne.k
      // Add scaled lambda to the diagonals of AtA.
      val scaledlambda = lambda * ne.n
      var i = 0
      var j = 2
      while (i < ne.triK) {
        ne.ata(i) += scaledlambda
        i += j
        j += 1
      }
      lapack.dppsv(upper, k, 1, ne.ata, ne.atb, k, info)
      val code = info.`val`
      assert(code == 0, s"lapack.dppsv returned $code.")
      val x = new Array[Float](k)
      i = 0
      while (i < k) {
        x(i) = ne.atb(i).toFloat
        i += 1
      }
      ne.reset()
      x
    }
  }

  /** Representing a normal equation (ALS' subproblem). */
  private[recommendation] class NormalEquation(val k: Int) extends Serializable {

    /** Number of entries in the upper triangular part of a k-by-k matrix. */
    val triK = k * (k + 1) / 2
    /** A^T^ * A */
    val ata = new Array[Double](triK)
    /** A^T^ * b */
    val atb = new Array[Double](k)
    /** Number of observations. */
    var n = 0

    private val da = new Array[Double](k)
    private val upper = "U"

    private def copyToDouble(a: Array[Float]): Unit = {
      var i = 0
      while (i < k) {
        da(i) = a(i)
        i += 1
      }
    }

    /** Adds an observation. */
    def add(a: Array[Float], b: Float): this.type = {
      require(a.size == k)
      copyToDouble(a)
      blas.dspr(upper, k, 1.0, da, 1, ata)
      blas.daxpy(k, b.toDouble, da, 1, atb, 1)
      n += 1
      this
    }

    /**
     * Adds an observation with implicit feedback. Note that this does not increment the counter.
     */
    def addImplicit(a: Array[Float], b: Float, alpha: Double): this.type = {
      require(a.size == k)
      // Extension to the original paper to handle b < 0. confidence is a function of |b| instead
      // so that it is never negative.
      val confidence = 1.0 + alpha * math.abs(b)
      copyToDouble(a)
      blas.dspr(upper, k, confidence - 1.0, da, 1, ata)
      // For b <= 0, the corresponding preference is 0. So the term below is only added for b > 0.
      if (b > 0) {
        blas.daxpy(k, confidence, da, 1, atb, 1)
      }
      this
    }

    /** Merges another normal equation object. */
    def merge(other: NormalEquation): this.type = {
      require(other.k == k)
      blas.daxpy(ata.size, 1.0, other.ata, 1, ata, 1)
      blas.daxpy(atb.size, 1.0, other.atb, 1, atb, 1)
      n += other.n
      this
    }

    /** Resets everything to zero, which should be called after each solve. */
    def reset(): Unit = {
      ju.Arrays.fill(ata, 0.0)
      ju.Arrays.fill(atb, 0.0)
      n = 0
    }
  }

  /**
   * Implementation of the ALS algorithm.
   */
  private def train(
      ratings: RDD[Rating],
      rank: Int = 10,
      numUserBlocks: Int = 10,
      numItemBlocks: Int = 10,
      maxIter: Int = 10,
      regParam: Double = 1.0,
      implicitPrefs: Boolean = false,
      alpha: Double = 1.0): (RDD[(Int, Array[Float])], RDD[(Int, Array[Float])]) = {
    val userPart = new HashPartitioner(numUserBlocks)
    val itemPart = new HashPartitioner(numItemBlocks)
    val userLocalIndexEncoder = new LocalIndexEncoder(userPart.numPartitions)
    val itemLocalIndexEncoder = new LocalIndexEncoder(itemPart.numPartitions)
    val blockRatings = partitionRatings(ratings, userPart, itemPart).cache()
    val (userInBlocks, userOutBlocks) = makeBlocks("user", blockRatings, userPart, itemPart)
    // materialize blockRatings and user blocks
    userOutBlocks.count()
    val swappedBlockRatings = blockRatings.map {
      case ((userBlockId, itemBlockId), RatingBlock(userIds, itemIds, localRatings)) =>
        ((itemBlockId, userBlockId), RatingBlock(itemIds, userIds, localRatings))
    }
    val (itemInBlocks, itemOutBlocks) = makeBlocks("item", swappedBlockRatings, itemPart, userPart)
    // materialize item blocks
    itemOutBlocks.count()
    var userFactors = initialize(userInBlocks, rank)
    var itemFactors = initialize(itemInBlocks, rank)
    if (implicitPrefs) {
      for (iter <- 1 to maxIter) {
        userFactors.setName(s"userFactors-$iter").persist()
        val previousItemFactors = itemFactors
        itemFactors = computeFactors(userFactors, userOutBlocks, itemInBlocks, rank, regParam,
          userLocalIndexEncoder, implicitPrefs, alpha)
        previousItemFactors.unpersist()
        itemFactors.setName(s"itemFactors-$iter").persist()
        val previousUserFactors = userFactors
        userFactors = computeFactors(itemFactors, itemOutBlocks, userInBlocks, rank, regParam,
          itemLocalIndexEncoder, implicitPrefs, alpha)
        previousUserFactors.unpersist()
      }
    } else {
      for (iter <- 0 until maxIter) {
        itemFactors = computeFactors(userFactors, userOutBlocks, itemInBlocks, rank, regParam,
          userLocalIndexEncoder)
        userFactors = computeFactors(itemFactors, itemOutBlocks, userInBlocks, rank, regParam,
          itemLocalIndexEncoder)
      }
    }
    val userIdAndFactors = userInBlocks
      .mapValues(_.srcIds)
      .join(userFactors)
      .values
      .setName("userFactors")
      .cache()
    userIdAndFactors.count()
    itemFactors.unpersist()
    val itemIdAndFactors = itemInBlocks
      .mapValues(_.srcIds)
      .join(itemFactors)
      .values
      .setName("itemFactors")
      .cache()
    itemIdAndFactors.count()
    userInBlocks.unpersist()
    userOutBlocks.unpersist()
    itemInBlocks.unpersist()
    itemOutBlocks.unpersist()
    blockRatings.unpersist()
    val userOutput = userIdAndFactors.flatMap { case (ids, factors) =>
      ids.view.zip(factors)
    }
    val itemOutput = itemIdAndFactors.flatMap { case (ids, factors) =>
      ids.view.zip(factors)
    }
    (userOutput, itemOutput)
  }

  /**
   * Factor block that stores factors (Array[Float]) in an Array.
   */
  private type FactorBlock = Array[Array[Float]]

  /**
   * Out-link block that stores, for each dst (item/user) block, which src (user/item) factors to
   * send. For example, outLinkBlock(0) contains the local indices (not the original src IDs) of the
   * src factors in this block to send to dst block 0.
   */
  private type OutBlock = Array[Array[Int]]

  /**
   * In-link block for computing src (user/item) factors. This includes the original src IDs
   * of the elements within this block as well as encoded dst (item/user) indices and corresponding
   * ratings. The dst indices are in the form of (blockId, localIndex), which are not the original
   * dst IDs. To compute src factors, we expect receiving dst factors that match the dst indices.
   * For example, if we have an in-link record
   *
   * {srcId: 0, dstBlockId: 2, dstLocalIndex: 3, rating: 5.0},
   *
   * and assume that the dst factors are stored as dstFactors: Map[Int, Array[Array[Float]]], which
   * is a blockId to dst factors map, the corresponding dst factor of the record is dstFactor(2)(3).
   *
   * We use a CSC-like (compressed sparse column) format to store the in-link information. So we can
   * compute src factors one after another using only one normal equation instance.
   *
   * @param srcIds src ids (ordered)
   * @param dstPtrs dst pointers. Elements in range [dstPtrs(i), dstPtrs(i+1)) of dst indices and
   *                ratings are associated with srcIds(i).
   * @param dstEncodedIndices encoded dst indices
   * @param ratings ratings
   *
   * @see [[LocalIndexEncoder]]
   */
  private[recommendation] case class InBlock(
      srcIds: Array[Int],
      dstPtrs: Array[Int],
      dstEncodedIndices: Array[Int],
      ratings: Array[Float]) {
    /** Size of the block. */
    val size: Int = ratings.size

    require(dstEncodedIndices.size == size)
    require(dstPtrs.size == srcIds.size + 1)
  }

  /**
   * Initializes factors randomly given the in-link blocks.
   *
   * @param inBlocks in-link blocks
   * @param rank rank
   * @return initialized factor blocks
   */
  private def initialize(inBlocks: RDD[(Int, InBlock)], rank: Int): RDD[(Int, FactorBlock)] = {
    // Choose a unit vector uniformly at random from the unit sphere, but from the
    // "first quadrant" where all elements are nonnegative. This can be done by choosing
    // elements distributed as Normal(0,1) and taking the absolute value, and then normalizing.
    // This appears to create factorizations that have a slightly better reconstruction
    // (<1%) compared picking elements uniformly at random in [0,1].
    inBlocks.map { case (srcBlockId, inBlock) =>
      val random = new XORShiftRandom(srcBlockId)
      val factors = Array.fill(inBlock.srcIds.size) {
        val factor = Array.fill(rank)(random.nextGaussian().toFloat)
        val nrm = blas.snrm2(rank, factor, 1)
        blas.sscal(rank, 1.0f / nrm, factor, 1)
        factor
      }
      (srcBlockId, factors)
    }
  }

  /**
   * A rating block that contains src IDs, dst IDs, and ratings, stored in primitive arrays.
   */
  private[recommendation]
  case class RatingBlock(srcIds: Array[Int], dstIds: Array[Int], ratings: Array[Float]) {
    /** Size of the block. */
    val size: Int = srcIds.size
    require(dstIds.size == size)
    require(ratings.size == size)
  }

  /**
   * Builder for [[RatingBlock]]. [[mutable.ArrayBuilder]] is used to avoid boxing/unboxing.
   */
  private[recommendation] class RatingBlockBuilder extends Serializable {

    private val srcIds = mutable.ArrayBuilder.make[Int]
    private val dstIds = mutable.ArrayBuilder.make[Int]
    private val ratings = mutable.ArrayBuilder.make[Float]
    var size = 0

    /** Adds a rating. */
    def add(r: Rating): this.type = {
      size += 1
      srcIds += r.user
      dstIds += r.item
      ratings += r.rating
      this
    }

    /** Merges another [[RatingBlockBuilder]]. */
    def merge(other: RatingBlock): this.type = {
      size += other.srcIds.size
      srcIds ++= other.srcIds
      dstIds ++= other.dstIds
      ratings ++= other.ratings
      this
    }

    /** Builds a [[RatingBlock]]. */
    def build(): RatingBlock = {
      RatingBlock(srcIds.result(), dstIds.result(), ratings.result())
    }
  }

  /**
   * Partitions raw ratings into blocks.
   *
   * @param ratings raw ratings
   * @param srcPart partitioner for src IDs
   * @param dstPart partitioner for dst IDs
   *
   * @return an RDD of rating blocks in the form of ((srcBlockId, dstBlockId), ratingBlock)
   */
  private def partitionRatings(
      ratings: RDD[Rating],
      srcPart: Partitioner,
      dstPart: Partitioner): RDD[((Int, Int), RatingBlock)] = {

     /* The implementation produces the same result as the following but generates less objects.

     ratings.map { r =>
       ((srcPart.getPartition(r.user), dstPart.getPartition(r.item)), r)
     }.aggregateByKey(new RatingBlockBuilder)(
         seqOp = (b, r) => b.add(r),
         combOp = (b0, b1) => b0.merge(b1.build()))
       .mapValues(_.build())
     */

    val numPartitions = srcPart.numPartitions * dstPart.numPartitions
    ratings.mapPartitions { iter =>
      val builders = Array.fill(numPartitions)(new RatingBlockBuilder)
      iter.flatMap { r =>
        val srcBlockId = srcPart.getPartition(r.user)
        val dstBlockId = dstPart.getPartition(r.item)
        val idx = srcBlockId + srcPart.numPartitions * dstBlockId
        val builder = builders(idx)
        builder.add(r)
        if (builder.size >= 2048) { // 2048 * (3 * 4) = 24k
          builders(idx) = new RatingBlockBuilder
          Iterator.single(((srcBlockId, dstBlockId), builder.build()))
        } else {
          Iterator.empty
        }
      } ++ {
        builders.view.zipWithIndex.filter(_._1.size > 0).map { case (block, idx) =>
          val srcBlockId = idx % srcPart.numPartitions
          val dstBlockId = idx / srcPart.numPartitions
          ((srcBlockId, dstBlockId), block.build())
        }
      }
    }.groupByKey().mapValues { blocks =>
      val builder = new RatingBlockBuilder
      blocks.foreach(builder.merge)
      builder.build()
    }.setName("ratingBlocks")
  }

  /**
   * Builder for uncompressed in-blocks of (srcId, dstEncodedIndex, rating) tuples.
   * @param encoder encoder for dst indices
   */
  private[recommendation] class UncompressedInBlockBuilder(encoder: LocalIndexEncoder) {

    private val srcIds = mutable.ArrayBuilder.make[Int]
    private val dstEncodedIndices = mutable.ArrayBuilder.make[Int]
    private val ratings = mutable.ArrayBuilder.make[Float]

    /**
     * Adds a dst block of (srcId, dstLocalIndex, rating) tuples.
     *
     * @param dstBlockId dst block ID
     * @param srcIds original src IDs
     * @param dstLocalIndices dst local indices
     * @param ratings ratings
     */
    def add(
        dstBlockId: Int,
        srcIds: Array[Int],
        dstLocalIndices: Array[Int],
        ratings: Array[Float]): this.type = {
      val sz = srcIds.size
      require(dstLocalIndices.size == sz)
      require(ratings.size == sz)
      this.srcIds ++= srcIds
      this.ratings ++= ratings
      var j = 0
      while (j < sz) {
        this.dstEncodedIndices += encoder.encode(dstBlockId, dstLocalIndices(j))
        j += 1
      }
      this
    }

    /** Builds a [[UncompressedInBlock]]. */
    def build(): UncompressedInBlock = {
      new UncompressedInBlock(srcIds.result(), dstEncodedIndices.result(), ratings.result())
    }
  }

  /**
   * A block of (srcId, dstEncodedIndex, rating) tuples stored in primitive arrays.
   */
  private[recommendation] class UncompressedInBlock(
      val srcIds: Array[Int],
      val dstEncodedIndices: Array[Int],
      val ratings: Array[Float]) {

    /** Size the of block. */
    def size: Int = srcIds.size

    /**
     * Compresses the block into an [[InBlock]]. The algorithm is the same as converting a
     * sparse matrix from coordinate list (COO) format into compressed sparse column (CSC) format.
     * Sorting is done using Spark's built-in Timsort to avoid generating too many objects.
     */
    def compress(): InBlock = {
      val sz = size
      assert(sz > 0, "Empty in-link block should not exist.")
      sort()
      val uniqueSrcIdsBuilder = mutable.ArrayBuilder.make[Int]
      val dstCountsBuilder = mutable.ArrayBuilder.make[Int]
      var preSrcId = srcIds(0)
      uniqueSrcIdsBuilder += preSrcId
      var curCount = 1
      var i = 1
      var j = 0
      while (i < sz) {
        val srcId = srcIds(i)
        if (srcId != preSrcId) {
          uniqueSrcIdsBuilder += srcId
          dstCountsBuilder += curCount
          preSrcId = srcId
          j += 1
          curCount = 0
        }
        curCount += 1
        i += 1
      }
      dstCountsBuilder += curCount
      val uniqueSrcIds = uniqueSrcIdsBuilder.result()
      val numUniqueSrdIds = uniqueSrcIds.size
      val dstCounts = dstCountsBuilder.result()
      val dstPtrs = new Array[Int](numUniqueSrdIds + 1)
      var sum = 0
      i = 0
      while (i < numUniqueSrdIds) {
        sum += dstCounts(i)
        i += 1
        dstPtrs(i) = sum
      }
      InBlock(uniqueSrcIds, dstPtrs, dstEncodedIndices, ratings)
    }

    private def sort(): Unit = {
      val sz = size
      // Since there might be interleaved log messages, we insert a unique id for easy pairing.
      val sortId = Utils.random.nextInt()
      logDebug(s"Start sorting an uncompressed in-block of size $sz. (sortId = $sortId)")
      val start = System.nanoTime()
      val sorter = new Sorter(new UncompressedInBlockSort)
      sorter.sort(this, 0, size, Ordering[IntWrapper])
      val duration = (System.nanoTime() - start) / 1e9
      logDebug(s"Sorting took $duration seconds. (sortId = $sortId)")
    }
  }

  /**
   * A wrapper that holds a primitive integer key.
   *
   * @see [[UncompressedInBlockSort]]
   */
  private class IntWrapper(var key: Int = 0) extends Ordered[IntWrapper] {
    override def compare(that: IntWrapper): Int = {
      key.compare(that.key)
    }
  }

  /**
   * [[SortDataFormat]] of [[UncompressedInBlock]] used by [[Sorter]].
   */
  private class UncompressedInBlockSort extends SortDataFormat[IntWrapper, UncompressedInBlock] {

    override def newKey(): IntWrapper = new IntWrapper()

    override def getKey(
        data: UncompressedInBlock,
        pos: Int,
        reuse: IntWrapper): IntWrapper = {
      if (reuse == null) {
        new IntWrapper(data.srcIds(pos))
      } else {
        reuse.key = data.srcIds(pos)
        reuse
      }
    }

    override def getKey(
        data: UncompressedInBlock,
        pos: Int): IntWrapper = {
      getKey(data, pos, null)
    }

    private def swapElements[@specialized(Int, Float) T](
        data: Array[T],
        pos0: Int,
        pos1: Int): Unit = {
      val tmp = data(pos0)
      data(pos0) = data(pos1)
      data(pos1) = tmp
    }

    override def swap(data: UncompressedInBlock, pos0: Int, pos1: Int): Unit = {
      swapElements(data.srcIds, pos0, pos1)
      swapElements(data.dstEncodedIndices, pos0, pos1)
      swapElements(data.ratings, pos0, pos1)
    }

    override def copyRange(
        src: UncompressedInBlock,
        srcPos: Int,
        dst: UncompressedInBlock,
        dstPos: Int,
        length: Int): Unit = {
      System.arraycopy(src.srcIds, srcPos, dst.srcIds, dstPos, length)
      System.arraycopy(src.dstEncodedIndices, srcPos, dst.dstEncodedIndices, dstPos, length)
      System.arraycopy(src.ratings, srcPos, dst.ratings, dstPos, length)
    }

    override def allocate(length: Int): UncompressedInBlock = {
      new UncompressedInBlock(
        new Array[Int](length), new Array[Int](length), new Array[Float](length))
    }

    override def copyElement(
        src: UncompressedInBlock,
        srcPos: Int,
        dst: UncompressedInBlock,
        dstPos: Int): Unit = {
      dst.srcIds(dstPos) = src.srcIds(srcPos)
      dst.dstEncodedIndices(dstPos) = src.dstEncodedIndices(srcPos)
      dst.ratings(dstPos) = src.ratings(srcPos)
    }
  }

  /**
   * Creates in-blocks and out-blocks from rating blocks.
   * @param prefix prefix for in/out-block names
   * @param ratingBlocks rating blocks
   * @param srcPart partitioner for src IDs
   * @param dstPart partitioner for dst IDs
   * @return (in-blocks, out-blocks)
   */
  private def makeBlocks(
      prefix: String,
      ratingBlocks: RDD[((Int, Int), RatingBlock)],
      srcPart: Partitioner,
      dstPart: Partitioner): (RDD[(Int, InBlock)], RDD[(Int, OutBlock)]) = {
    val inBlocks = ratingBlocks.map {
      case ((srcBlockId, dstBlockId), RatingBlock(srcIds, dstIds, ratings)) =>
        // The implementation is a faster version of
        // val dstIdToLocalIndex = dstIds.toSet.toSeq.sorted.zipWithIndex.toMap
        val start = System.nanoTime()
        val dstIdSet = new OpenHashSet[Int](1 << 20)
        dstIds.foreach(dstIdSet.add)
        val sortedDstIds = new Array[Int](dstIdSet.size)
        var i = 0
        var pos = dstIdSet.nextPos(0)
        while (pos != -1) {
          sortedDstIds(i) = dstIdSet.getValue(pos)
          pos = dstIdSet.nextPos(pos + 1)
          i += 1
        }
        assert(i == dstIdSet.size)
        ju.Arrays.sort(sortedDstIds)
        val dstIdToLocalIndex = new OpenHashMap[Int, Int](sortedDstIds.size)
        i = 0
        while (i < sortedDstIds.size) {
          dstIdToLocalIndex.update(sortedDstIds(i), i)
          i += 1
        }
        logDebug(
          "Converting to local indices took " + (System.nanoTime() - start) / 1e9 + " seconds.")
        val dstLocalIndices = dstIds.map(dstIdToLocalIndex.apply)
        (srcBlockId, (dstBlockId, srcIds, dstLocalIndices, ratings))
    }.groupByKey(new HashPartitioner(srcPart.numPartitions))
        .mapValues { iter =>
      val builder =
        new UncompressedInBlockBuilder(new LocalIndexEncoder(dstPart.numPartitions))
      iter.foreach { case (dstBlockId, srcIds, dstLocalIndices, ratings) =>
        builder.add(dstBlockId, srcIds, dstLocalIndices, ratings)
      }
      builder.build().compress()
    }.setName(prefix + "InBlocks").cache()
    val outBlocks = inBlocks.mapValues { case InBlock(srcIds, dstPtrs, dstEncodedIndices, _) =>
      val encoder = new LocalIndexEncoder(dstPart.numPartitions)
      val activeIds = Array.fill(dstPart.numPartitions)(mutable.ArrayBuilder.make[Int])
      var i = 0
      val seen = new Array[Boolean](dstPart.numPartitions)
      while (i < srcIds.size) {
        var j = dstPtrs(i)
        ju.Arrays.fill(seen, false)
        while (j < dstPtrs(i + 1)) {
          val dstBlockId = encoder.blockId(dstEncodedIndices(j))
          if (!seen(dstBlockId)) {
            activeIds(dstBlockId) += i // add the local index in this out-block
            seen(dstBlockId) = true
          }
          j += 1
        }
        i += 1
      }
      activeIds.map { x =>
        x.result()
      }
    }.setName(prefix + "OutBlocks").cache()
    (inBlocks, outBlocks)
  }

  /**
   * Compute dst factors by constructing and solving least square problems.
   *
   * @param srcFactorBlocks src factors
   * @param srcOutBlocks src out-blocks
   * @param dstInBlocks dst in-blocks
   * @param rank rank
   * @param regParam regularization constant
   * @param srcEncoder encoder for src local indices
   * @param implicitPrefs whether to use implicit preference
   * @param alpha the alpha constant in the implicit preference formulation
   *
   * @return dst factors
   */
  private def computeFactors(
      srcFactorBlocks: RDD[(Int, FactorBlock)],
      srcOutBlocks: RDD[(Int, OutBlock)],
      dstInBlocks: RDD[(Int, InBlock)],
      rank: Int,
      regParam: Double,
      srcEncoder: LocalIndexEncoder,
      implicitPrefs: Boolean = false,
      alpha: Double = 1.0): RDD[(Int, FactorBlock)] = {
    val numSrcBlocks = srcFactorBlocks.partitions.size
    val YtY = if (implicitPrefs) Some(computeYtY(srcFactorBlocks, rank)) else None
    val srcOut = srcOutBlocks.join(srcFactorBlocks).flatMap {
      case (srcBlockId, (srcOutBlock, srcFactors)) =>
        srcOutBlock.view.zipWithIndex.map { case (activeIndices, dstBlockId) =>
          (dstBlockId, (srcBlockId, activeIndices.map(idx => srcFactors(idx))))
        }
    }
    val merged = srcOut.groupByKey(new HashPartitioner(dstInBlocks.partitions.size))
    dstInBlocks.join(merged).mapValues {
      case (InBlock(dstIds, srcPtrs, srcEncodedIndices, ratings), srcFactors) =>
        val sortedSrcFactors = new Array[FactorBlock](numSrcBlocks)
        srcFactors.foreach { case (srcBlockId, factors) =>
          sortedSrcFactors(srcBlockId) = factors
        }
        val dstFactors = new Array[Array[Float]](dstIds.size)
        var j = 0
        val ls = new NormalEquation(rank)
        val solver = new CholeskySolver // TODO: add NNLS solver
        while (j < dstIds.size) {
          ls.reset()
          if (implicitPrefs) {
            ls.merge(YtY.get)
          }
          var i = srcPtrs(j)
          while (i < srcPtrs(j + 1)) {
            val encoded = srcEncodedIndices(i)
            val blockId = srcEncoder.blockId(encoded)
            val localIndex = srcEncoder.localIndex(encoded)
            val srcFactor = sortedSrcFactors(blockId)(localIndex)
            val rating = ratings(i)
            if (implicitPrefs) {
              ls.addImplicit(srcFactor, rating, alpha)
            } else {
              ls.add(srcFactor, rating)
            }
            i += 1
          }
          dstFactors(j) = solver.solve(ls, regParam)
          j += 1
        }
        dstFactors
    }
  }

  /**
   * Computes the Gramian matrix of user or item factors, which is only used in implicit preference.
   * Caching of the input factors is handled in [[ALS#train]].
   */
  private def computeYtY(factorBlocks: RDD[(Int, FactorBlock)], rank: Int): NormalEquation = {
    factorBlocks.values.aggregate(new NormalEquation(rank))(
      seqOp = (ne, factors) => {
        factors.foreach(ne.add(_, 0.0f))
        ne
      },
      combOp = (ne1, ne2) => ne1.merge(ne2))
  }

  /**
   * Encoder for storing (blockId, localIndex) into a single integer.
   *
   * We use the leading bits (including the sign bit) to store the block id and the rest to store
   * the local index. This is based on the assumption that users/items are approximately evenly
   * partitioned. With this assumption, we should be able to encode two billion distinct values.
   *
   * @param numBlocks number of blocks
   */
  private[recommendation] class LocalIndexEncoder(numBlocks: Int) extends Serializable {

    require(numBlocks > 0, s"numBlocks must be positive but found $numBlocks.")

    private[this] final val numLocalIndexBits =
      math.min(java.lang.Integer.numberOfLeadingZeros(numBlocks - 1), 31)
    private[this] final val localIndexMask = (1 << numLocalIndexBits) - 1

    /** Encodes a (blockId, localIndex) into a single integer. */
    def encode(blockId: Int, localIndex: Int): Int = {
      require(blockId < numBlocks)
      require((localIndex & ~localIndexMask) == 0)
      (blockId << numLocalIndexBits) | localIndex
    }

    /** Gets the block id from an encoded index. */
    @inline
    def blockId(encoded: Int): Int = {
      encoded >>> numLocalIndexBits
    }

    /** Gets the local index from an encoded index. */
    @inline
    def localIndex(encoded: Int): Int = {
      encoded & localIndexMask
    }
  }
}
