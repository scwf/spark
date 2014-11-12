package org.apache.spark.sql.hbase.logical

import org.apache.spark.sql.catalyst.plans.logical.{UnaryNode, LeafNode, LogicalPlan, Command}
//import org.apache.spark.sql.hbase.HBaseRelation
/*
case class CreateHBaseTablePlan(tableName: String,
                                nameSpace: String,
                                hbaseTable: String,
                                colsSeq: Seq[String],
                                keyCols: Seq[(String, String)],
                                nonKeyCols: Seq[(String, String, String, String)]
                                 ) extends Command

case class DropTablePlan(tableName: String) extends Command

case class BulkLoadIntoTable(
    table: HBaseRelation,
    path: String) extends LeafNode  {
  override def output = Seq.empty
  // TODO:need resolved here?

}
*/
case class LoadDataIntoTable(path: String,
                             child: LogicalPlan,
                             isLocal: Boolean) extends UnaryNode {

  override def output = Nil

  override def toString = s"LogicalPlan: LoadDataIntoTable(LOAD $path INTO $child)"
}

