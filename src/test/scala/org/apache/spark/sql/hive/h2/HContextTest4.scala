package org.apache.spark.sql.hive.h2

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by w00297350 on 2014/11/29.
 */
object HContextTest4 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("myHContext").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val hContext = new HContext(sc)
    hContext.setConf("spark.sql.dialect","hiveql")
    val ret=hContext.sql("call a")

    println(ret.collect().length)

    println()


  }
}
