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
    hContext.setConf("spark.sql.dialect","h2ql")
    val ret=hContext.sql("select name from emp where age>1 and depno=1 and name='aaa' or age>10")

    ret.collect().foreach(row=>println(row.getString(0)))

    //println()


  }
}
