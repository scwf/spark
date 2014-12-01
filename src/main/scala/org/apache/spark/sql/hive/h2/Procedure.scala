package org.apache.spark.sql.hive.h2

import org.apache.spark.sql._


/**
 * Created by w00297350 on 2014/11/29.
 */
class Procedure {

  def test(hContext:HContext): SchemaRDD =
  {
    val rdd1=hContext.sql("select name, age, depno from emp where age > 1 and name = 'aaa'")

    val rdd2=hContext.sql("select depno from dep")

    val ret=rdd1.filter(row =>{
      val depno=row.getInt(2)
       if(depno==1)
       {
         true
       }
      else
       {
         false
       }
    })

    val sparkContext=hContext.sparkContext
    val seq=Seq[String]("a","b")
    val kk=sparkContext.parallelize(seq).map(line=>Row(line))

    val structFields = new Array[StructField](1)
    structFields.update(0, StructField("NAME", StringType, true))
    val kkSchema = StructType(structFields)

    val dd=hContext.applySchema(kk,kkSchema)

    dd
  }

}
