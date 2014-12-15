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
    val ret=hContext.sql("select  name, age, emp.depno, dep2.depno, dep2.depname from emp inner join dep2 on emp.depno=dep2.depno")
    println(ret.logicalPlan)
//    val ret2=ret.collect()
//    println(ret2.size)

//    println(ret.printSchema())
    println(ret.queryExecution.toString)

    ret.collect().foreach(row =>
    {
      print(if(row.isNullAt(0)) "null" else row.getString(0))
      print("\t")
      print(if(row.isNullAt(1)) "null" else row.getInt(1))
      print("\t")
      print(if(row.isNullAt(2)) "null" else row.getInt(2))
      print("\t")
      print(if(row.isNullAt(3)) "null" else row.getInt(3))
      print("\t")
      print(if(row.isNullAt(4)) "null" else row.getString(4))
      println("\t")
    }
    )
//    ret2.foreach(row => println(row.getInt(0)+"\t"
//      //+row.getInt(1)
//    ))

  }
}
