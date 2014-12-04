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
    hContext.setConf("spark.sql.dialect","sql")
    hContext.setConf("spark.sql.shuffle.partitions","1")
    val ret=hContext.sql("select min(age) as minage  from emp group by depno having minage>10 order by minage desc ")
    val ret2=ret.collect()
    println(ret2.size)

    println(ret.printSchema())
    println(ret.queryExecution.toString)
    ret2.foreach(row => println(row.getInt(0)+"\t"
      //+row.getInt(1)
    ))

  }
}
