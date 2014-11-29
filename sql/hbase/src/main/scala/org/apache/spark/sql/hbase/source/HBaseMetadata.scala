package org.apache.spark.sql.hbase.source

import java.io._
import scala.Some

import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.sql.hbase._
import org.apache.spark.Logging
import org.apache.spark.sql.hbase.HBaseRelation
import org.apache.spark.sql.hbase.NonKeyColumn

private[source] class HBaseMetadata extends Logging with Serializable {

  lazy val configuration = HBaseConfiguration.create()

  lazy val admin = new HBaseAdmin(configuration)

  logDebug(s"HBaseAdmin.configuration zkPort="
    + s"${admin.getConfiguration.get("hbase.zookeeper.property.clientPort")}")

  private def createHBaseUserTable(tableName: String, allColumns: Seq[AbstractColumn]) {
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    allColumns.map(x =>
      if (x.isInstanceOf[NonKeyColumn]) {
        val nonKeyColumn = x.asInstanceOf[NonKeyColumn]
        tableDescriptor.addFamily(new HColumnDescriptor(nonKeyColumn.family))
      })

    admin.createTable(tableDescriptor, null);
  }

  def createTable(
      tableName: String,
      hbaseTableName: String,
      allColumns: Seq[AbstractColumn]) = {
    // create a new hbase table for the user if not exist
    if (!checkHBaseTableExists(hbaseTableName)) {
      createHBaseUserTable(hbaseTableName, allColumns)
    }
    // check hbase table contain all the families
    val nonKeyColumns = allColumns.filter(_.isInstanceOf[NonKeyColumn])
    nonKeyColumns.foreach {
      case NonKeyColumn(_, _, family, _) =>
        if (!checkFamilyExists(hbaseTableName, family)) {
          throw new Exception(s"The HBase table doesn't contain the Column Family: $family")
        }
    }

    HBaseRelation(tableName, "", hbaseTableName, allColumns, Some(configuration))
  }

  private[hbase] def checkHBaseTableExists(hbaseTableName: String): Boolean = {
    admin.tableExists(hbaseTableName)
  }

  private[hbase] def checkFamilyExists(hbaseTableName: String, family: String): Boolean = {
    val tableDescriptor = admin.getTableDescriptor(TableName.valueOf(hbaseTableName))
    tableDescriptor.hasFamily(Bytes.toBytes(family))
  }
}
