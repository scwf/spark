package org.apache.spark.sql.hive.h2

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by w00297350 on 2014/11/29.
 */
object HContextTest3 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("myHContext").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val hContext = new HContext(sc)
    hContext.setConf("spark.sql.dialect","hiveql")
    val pro=new Procedure
    val rdd=pro.test(hContext)
    println(rdd.printSchema())
    println(rdd.queryExecution.toRdd)
    println(rdd.count())
    val dataTypes = rdd.queryExecution.analyzed.output.map(_.dataType).toArray
    println(rdd.queryExecution.logical)

    dataTypes.foreach(d=>println(d))

  }
}
