package org.apache.spark.sql.hive.h2

import org.apache.spark.sql._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by w00297350 on 2014/11/27.
 */
object HContextTest1 {

  def main(args: Array[String]) {
    println("test my h2sql grammar")
    val sparkConf = new SparkConf().setAppName("aaa").setMaster("local")
    val sc = new SparkContext(sparkConf)

    val hContext = new HContext(sc)
    import hContext._
    import hContext.createSchemaRDD

    hContext.setConf("spark.sql.dialect","hiveql")
    val structFields = new Array[StructField](3)
    structFields.update(0, StructField("NAME", StringType, true))
    structFields.update(1, StructField("AGE", IntegerType, true))
    structFields.update(2, StructField("depno", IntegerType, true))
    val empSchema = StructType(structFields)
    val empRdd=sc.textFile("D:\\SparkSQL\\testdata\\emp.txt").map(line=>
    {
      val dataArr=line.split(" ")
      Row(dataArr(0), dataArr(1).toInt,dataArr(2).toInt)
    }
    )
    val empSchemaRdd=hContext.applySchema(empRdd,empSchema);
    hContext.registerRDDAsTable(empSchemaRdd,"EMP")

    val ret=hContext.sql("select name , age from emp limit 20")
    ret.collect().foreach(r=> (println(r.getString(0)+","+r.getInt(1))))

    //ret.printSchema()
  }

}
