package org.apache.spark.sql.hbase.source

import org.apache.spark.sql.sources.{BaseRelation, RelationProvider}
import org.apache.spark.sql.SQLContext

/**
 * Allows creation of parquet based tables using the syntax
 * `CREATE TEMPORARY TABLE table_name
 *  USING org.apache.spark.sql.hbase.source
 *  OPTIONS (
 *    hbase.table hbase_table_name,
 *    fileds (field1 filed1_type, filed2 filed2_type...),
 *    mapping (filed1=cf1.column1, filed2=cf2.column2...)
 *    primary.key filed_name
 *  )`.
 */
class DefaultSource extends RelationProvider {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
       sqlContext: SQLContext,
       parameters: Map[String, String]): BaseRelation = {
    val hbaseTableName =
    val  =
      parameters.getOrElse("spark.sql.hbase.conf.path",
        sys.error("'spark.sql.hbase.conf.path' must be specified for parquet tables."))

  }
}
