package org.apache.spark.sql.hive.h2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._

/**
 * Created by w00297350 on 2014/11/13.
 */
object MySparkSqlTest {

  def main(args: Array[String]): Unit = {
    System.out.println("the sql analysis start:");
    val sparkConf = new SparkConf().setAppName("CheckIn Analysis")
    sparkConf.setMaster("local")
    sparkConf.set("spark.sql.dialect", "sql") //hiveql
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext._
    import sqlContext.createSchemaRDD

    //个人信息表
    val structFields = new Array[StructField](3)
    structFields.update(0, StructField("zjhm", StringType, true))
    structFields.update(1, StructField("sex", StringType, true))
    structFields.update(2, StructField("height", IntegerType, true))
    val zcinfoSchema = StructType(structFields)

    val zcinfoRdd=sc.textFile("D:\\sparkdemo\\data\\ZC_INFO.txt")
      .map(
        line=>
        {
          val dataArr=line.split(" ")
          Row(dataArr(0),dataArr(1),dataArr(2).toInt)

        }
      )

    val zcinfoSchemaRdd=sqlContext.applySchema(zcinfoRdd,zcinfoSchema)
    zcinfoSchemaRdd.registerTempTable("zcinfo")
    val zcinfoRetRdd=sqlContext.sql("select zjhm,sex,height from zcinfo where sex='male' and height>170")
    zcinfoRetRdd.collect();
  }

}
