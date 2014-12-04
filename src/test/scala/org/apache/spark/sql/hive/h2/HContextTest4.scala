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
    hContext.setConf("spark.sql.shuffle.partitions","1")
    val ret=hContext.sql("select min(age) as minage  from emp group by depno order by minage desc")

    ret.printSchema()
    println(ret.queryExecution.toString)
    ret.collect().foreach(row=>println(row.getInt(0)))

    //println()

  }
}
