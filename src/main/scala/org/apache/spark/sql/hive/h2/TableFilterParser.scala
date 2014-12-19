package org.apache.spark.sql.hive.h2

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{LeftOuter, JoinType, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, Join, LogicalPlan}
import org.apache.spark.sql.hive.h2.expression.H2ExpressionParser
import org.h2.command.dml.Select
import org.h2.mvstore.db.MVTable
import org.h2.table.{TableView, Table, TableFilter}

/**
 * Created by w00297350 on 2014/12/15.
 */
object TableFilterParser {

  //解析子查询(难点)和join操作
  def apply(topTableFilter: TableFilter): LogicalPlan={
    var join:TableFilter=topTableFilter.join;
    var current_TableFilter=topTableFilter
    if(join==null)
    {
      TableParser(topTableFilter)
    }
    else
    {
      //从上到下逐一合并logic plan，遇到子查询递归计算select的逻辑计划
      var r1:LogicalPlan=null
      var r2:LogicalPlan=null
      while(join!=null)
      {
        if(r1==null) r1=TableParser(current_TableFilter)

        //right join时候会引入nestjoin, 对应的表会在nestjoin中
        if(join.nestedJoin==null)
        {
          r2 = TableParser(join)
        }
        else
        {
          r2 =TableParser(join.nestedJoin)
        }

        //从上到下处理Join操, 逐一合并
        var joinType:JoinType=null;

        //h2解析sql过程中，会把right join的表移到左边
        if(join.joinOuter) joinType=LeftOuter else joinType=Inner
        var joinExpr:Option[Expression]=null
        if(join.joinCondition==null) joinExpr=None else joinExpr=Option(H2ExpressionParser(join.joinCondition))
        r1=Join(r1,r2,joinType,joinExpr)

        current_TableFilter=join
        join=current_TableFilter.join
      }
      return r1
    }
  }

}

object TableParser
{
  def apply(table:Table, alias:Option[String]) : LogicalPlan=
  {
    table match
    {
      case mvtable:MVTable =>
        return UnresolvedRelation(None, table.objectName, alias);

      //下面tableview对应于处理子查询
      case tableView:TableView =>
        tableView.viewQuery match
        {
          case viewSelect:Select =>
            return Subquery(alias.getOrElse(None.toString),SelectParser(viewSelect))
        }

      case _ =>
        sys.error("unsupport the table type parser")
    }
  }

  def apply(tableFilter: TableFilter): LogicalPlan=
  {
    var alias:Option[String]=null
    if(tableFilter.alias ==null) alias=None else alias=Option(tableFilter.alias)
    TableParser(tableFilter.table,alias)
  }
}
