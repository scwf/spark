package org.apache.spark.sql.hive.h2

import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.Suite

/**
 * Created by w00297350 on 2014/12/5.
 */
class HContextUnitTest extends Suite{
  val sparkConf = new SparkConf().setAppName("myHContext").setMaster("local")
  val sc = new SparkContext(sparkConf)
  val hcontext = new HContext(sc)
  hcontext.setConf("spark.sql.dialect","h2ql")
  hcontext.setConf("spark.sql.shuffle.partitions","1")

  def test1():Unit=
  {
    val ret=hcontext.sql("select name, age, depno from emp")
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
  def test2():Unit=
  {
    val ret=hcontext.sql("select name, age, depno from emp where name='aaa'")
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

  def test3():Unit=
  {
    val ret=hcontext.sql("select name, age, depno from emp where name='bbb' and age>10")
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

  def test4():Unit=
  {
    val ret=hcontext.sql("select name, age, depno from emp where name='ccc' or (depno=1 and age>20)")
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

  def test5():Unit=
  {
    val ret=hcontext.sql("select name, age, depno from emp where age>0 order by depno asc, age desc,name")
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


  def test6():Unit=
  {
    val ret=hcontext.sql("select name, age, depno from emp where age>0 order by depno desc, age desc,name")
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

  def test7():Unit=
  {
    val ret=hcontext.sql("select min(age) from emp ")
    ret.collect().foreach(row =>
    {
      print(row.getInt(0))
      print("\t")

    }
    )
  }

  def test8():Unit=
  {
    val ret=hcontext.sql("select min(age) from emp where age>10")
    ret.collect().foreach(row =>
    {
      print(row.getInt(0))
      print("\t")

    }
    )
  }

  def test9():Unit=
  {
    val ret=hcontext.sql("select min(age) from emp where age>0 group by depno")
    ret.collect().foreach(row =>
    {
      print(row.getInt(0))
      print("\t")

    }
    )
  }

}
