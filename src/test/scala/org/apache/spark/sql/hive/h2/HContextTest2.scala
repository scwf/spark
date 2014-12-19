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
    hContext.setConf("spark.sql.dialect","fisql")
    hContext.setConf("spark.sql.shuffle.partitions","1")
    //val sql="select * from (SELECT e.name,e.age,e.depno FROM (select * from (select name,age,ea.depno from emp ea left join dep ed on ea.depno=ed.depno) ee where ee.age>20) e where e.age>0) t1, emp t2 where t1.name=t2.name"
    //val sql="select  name, age, emp.depno, dep2.depno, dep2.depname from emp inner join (select depno, depname from dep2) as dep2 on emp.depno=dep2.depno"
    //val sql="select  name, age from emp inner join (select depno, depname from (select * from dep) as aa) as  dep2 on emp.depno=dep2.depno"
    //val sql="select  name, age from emp inner join (select depno, depname from (select * from dep)) as  dep2 on emp.depno=dep2.depno"
    //val sql="select age from emp where not exists (select depno from dep)"
    //val sql="select name, case age when 10 then 11 when 20 then 22 else 33 end as age2 from emp"
    //val sql="select name ,age from emp where age not between 10 and 20"
    //val sql= "with xxx as (select name from emp)"
    //val sql ="select min(age) from emp group by cube(depno,name)"
    //val sql="select age from emp minus select age from emp"
    val sql="select age, name from emp"
    println(sql)
    val ret=hContext.sql(sql)
    println(ret.logicalPlan)
    val ret2=ret.collect()
    println("size:"+ret2.size)


//    println(ret.printSchema())
    println(ret.queryExecution.toString)

    ret.collect().foreach(row =>
    {
      print(if(row.isNullAt(0)) "null" else row.getInt(0))
//      print("\t")
//      print(if(row.isNullAt(1)) "null" else row.getInt(1))
//      print("\t")
//      print(if(row.isNullAt(2)) "null" else row.getInt(2))
//      print("\t")
//      print(if(row.isNullAt(3)) "null" else row.getInt(3))
//      print("\t")
//      print(if(row.isNullAt(4)) "null" else row.getString(4))
      println("\t")
    }
    )

//    ret2.foreach(row => println(row.getInt(0)+"\t"
//      //+row.getInt(1)
//    ))

  }
}
