package org.apache.spark.sql.hive.h2

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.{LeftOuter, JoinType, Inner}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.hive.h2.expression.H2ExpressionParser
import org.h2.table.TableFilter

/**
 * Created by w00297350 on 2014/12/15.
 */
object TableFilterParser {

  def apply(topTableFilter: TableFilter): LogicalPlan={
    var join:TableFilter=topTableFilter.join;
    var current_TableFilter=topTableFilter
    if(join==null)
    {
      return UnresolvedRelation(None, topTableFilter.table.objectName, None);
    }
    else
    {
      var r1:LogicalPlan=null
      var r2:LogicalPlan=null
      while(join!=null)
      {
        if(r1==null) r1=UnresolvedRelation(None,current_TableFilter.table.objectName,None)

        //right join时候会引入nestjoin, 对应的表会在nestjoin中
        if(join.nestedJoin==null)
        {
          r2 = UnresolvedRelation(None, join.table.objectName, None)
        }
        else
        {
          r2 = UnresolvedRelation(None, join.nestedJoin.table.objectName, None)
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
