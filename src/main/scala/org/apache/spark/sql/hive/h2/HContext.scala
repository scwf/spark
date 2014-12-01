package org.apache.spark.sql.hive.h2

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{SQLConf, SchemaRDD}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.HiveQl

/**
 * wuwei add
 * Created by w00297350 on 2014/11/24.
 */
class HContext(sc: SparkContext) extends HiveContext(sc)  {
  self =>

  override private[spark] def dialect: String = getConf(SQLConf.DIALECT, "h2ql")

  @transient
  protected[sql] val parserH22 = new H2SqlParser
  protected[sql] def parseH2Sql2(sql: String): LogicalPlan = parserH22(sql)

  override def sql(sqlText: String): SchemaRDD = {
    // TODO: Create a framework for registering parsers instead of just hardcoding if statements.
    if (dialect == "sql") {
      super.sql(sqlText)
    } else if (dialect == "hiveql") {

      //call procedure
      if(sqlText.startsWith("call"))
      {
        val pro=new Procedure
        val rdd=pro.test(this)
        return rdd
      }

      new SchemaRDD(this, HiveQl.parseSql(sqlText))
    } else if(dialect == "h2ql") {

      //call procedure
      if(sqlText.startsWith("call"))
      {
        val pro=new Procedure
        val rdd=pro.test(this)
        return rdd
      }

      new SchemaRDD(this, parseH2Sql2(sqlText))

    }
    else {
      sys.error(s"Unsupported SQL dialect: $dialect.  Try 'sql' or 'hiveql' or 'h2ql'")
    }
  }
}

