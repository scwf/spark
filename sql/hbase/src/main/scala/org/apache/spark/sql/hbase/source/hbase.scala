package org.apache.spark.sql.hbase.source

import org.apache.spark.sql.sources.{CatalystScan, BaseRelation, RelationProvider}
import org.apache.spark.sql.SQLContext
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.hbase.AbstractColumn
import org.apache.spark.annotation.DeveloperApi

/**
 * Allows creation of parquet based tables using the syntax
 * `CREATE TEMPORARY TABLE table_name(field1 filed1_type, filed2 filed2_type...)
 *  USING org.apache.spark.sql.hbase.source
 *  OPTIONS (
 *    hbase.table hbase_table_name,
 *    mapping (filed1=cf1.column1, filed2=cf2.column2...)
 *    primary.key filed_name
 *  )`.
 */
class DefaultSource extends RelationProvider with Logging {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
       sqlContext: SQLContext,
       parameters: Map[String, String],
       schema: Option[StructType]): BaseRelation = {
    assert(schema.nonEmpty, "schema can not be empty for hbase rouce!")
    assert(parameters.get("hbase.table").nonEmpty, "no option for hbase.table")
    assert(parameters.get("mapping").nonEmpty, "no option for mapping")

    val hbaseTableName = parameters.getOrElse("hbase.table", "").toLowerCase
    val mapping = parameters.getOrElse("mapping", "").toLowerCase
    // todo: regrex to collect the map of filed and column

    // todo: check for mapping is legal

    // todo: rename to HBaseRelation
    HBaseRelation2(hbaseTableName, Seq.empty, schema.get)(sqlContext)
  }
}

@DeveloperApi
case class HBaseRelation2(
    hbaseTableName: String,
    allColumns: Seq[AbstractColumn],  // todo: refer to hbase relation and scan to design this
    schema: StructType)(sqlContext: SQLContext) extends CatalystScan with Logging {

}


