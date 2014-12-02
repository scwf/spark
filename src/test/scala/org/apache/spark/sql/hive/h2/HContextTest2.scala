package org.apache.spark.sql.hive.h2

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by w00297350 on 2014/11/29.
 */
object HContextTest2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("myHContext").setMaster("local").set("spark.logConf","true")
    val sc = new SparkContext(sparkConf)
    val hContext = new HContext(sc)
    import hContext._
    hContext.setConf("spark.sql.dialect","hiveql")
    val ret=hContext.sql("select * from emp")
    val ret2=ret.collect()
    println(ret2.size)

  }
}
