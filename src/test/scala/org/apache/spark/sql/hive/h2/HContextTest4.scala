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

    test1(hContext)
    test2(hContext)
    test3(hContext)
    test4(hContext)
    test5(hContext)
    test6(hContext)
    test7(hContext)
    test8(hContext)
    test9(hContext)
    test10(hContext)
    test11(hContext)

    //sc.stop()
  }

  //single table
  def test1(hcontext:HContext):Unit=
  {
    val sql="select name, age, depno from emp"
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
      {
         print(row.getString(0))
         print("\t")

        print(row.getInt(1))
        print("\t")

        print(row.getInt(2))
        println("\t")
      }
    )
  }

  //single table
  def test2(hcontext:HContext):Unit=
  {
    val sql="select name, age, depno from emp where name='aaa'"
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
    {
      print(row.getString(0))
      print("\t")

      print(row.getInt(1))
      print("\t")

      print(row.getInt(2))
      println("\t")
    }
    )
  }

  def test3(hcontext:HContext):Unit=
  {
    val sql="select name, age, depno from emp where name='bbb' and age>10"
    val ret=hcontext.sql(sql)
    println(sql)

    ret.collect().foreach(row =>
    {
      print(row.getString(0))
      print("\t")

      print(row.getInt(1))
      print("\t")

      print(row.getInt(2))
      println("\t")
    }
    )
  }

  def test4(hcontext:HContext):Unit=
  {
    val sql="select name, age, depno from emp where name='ccc' or (depno=1 and age>20)"
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
    {
      print(row.getString(0))
      print("\t")

      print(row.getInt(1))
      print("\t")

      print(row.getInt(2))
      println("\t")
    }
    )
  }

  def test5(hcontext:HContext):Unit=
  {
    val sql="select name, age, depno from emp where age>0 order by depno asc, age desc,name"
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
    {
      print(row.getString(0))
      print("\t")

      print(row.getInt(1))
      print("\t")

      print(row.getInt(2))
      println("\t")
    }
    )
  }


  def test6(hcontext:HContext):Unit=
  {
    val sql="select name, age, depno from emp where age>0 order by depno desc, age desc,name"
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
    {
      print(row.getString(0))
      print("\t")

      print(row.getInt(1))
      print("\t")

      print(row.getInt(2))
      println("\t")
    }
    )
  }

  def test7(hcontext:HContext):Unit=
  {
    val sql="select min(age) from emp "
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
    {
      print(row.getInt(0))
      println("\t")

    }
    )
  }

  def test8(hcontext:HContext):Unit=
  {
    val sql="select min(age) from emp where age>10"
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
    {
      print(row.getInt(0))
      println("\t")

    }
    )
  }

  def test9(hcontext:HContext):Unit=
  {
    val sql="select min(age) from emp where age>0 group by depno"
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
    {
      print(row.getInt(0))
      println("\t")

    }
    )
  }

  def test10(hcontext:HContext):Unit=
  {
    val sql="select  min(age) as minage ,depno from emp where age>0 group by depno order by minage desc"
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
    {
      print(row.getInt(0))
      println("\t")

    }
    )
  }

  def test11(hcontext:HContext):Unit=
  {
    val sql="select  min(age) minage, depno from emp where age>0 group by depno having depno=1 order by minage desc"
    val ret=hcontext.sql(sql)
    println(sql)
    ret.collect().foreach(row =>
    {
      print(row.getInt(0))
      println("\t")

    }
    )
  }

}
