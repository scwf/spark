package org.apache.spark.sql.hive.h2

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.hive.h2.expression.{SortOrderParser, H2ExpressionParser}
import org.h2.command.dml.Select
import org.h2.expression.Condition


/**
 * Created by w00297350 on 2014/12/16.
 */
object SelectParser {

  def apply(select:Select):LogicalPlan=
  {
    // relation
    val relation =if(select.topTableFilter==null) NoRelation else TableFilterParser(select.topTableFilter)

    //filter
    var conditionExpr:Expression=null;
    if(select.condition !=null) {
      val condition: Condition = select.condition.asInstanceOf[Condition]
      conditionExpr=H2ExpressionParser(condition)
    }
    val filter= if(conditionExpr!=null)  Filter(conditionExpr, relation) else relation

    //project and group
    var project:UnaryNode=null
    val expressions=select.getExpressions();
    val projectExpressions = ExpressionsParser(expressions)
    val groupExpr=select.groupExprForSpark
    if(groupExpr!=null)
    {
      val groupingExpressions=GroupParser(groupExpr)
      project=Aggregate(assignAliases(groupingExpressions), assignAliases(projectExpressions), filter)
    }
    else
    {
      project = Project(assignAliases(projectExpressions), filter)
    }

    //distinct
    val distinct= if(select.distinct) Distinct(project) else project

    //having
    val havingExprCondition=select.havingExprForSpark
    val having= if(havingExprCondition==null) distinct else Filter(H2ExpressionParser(havingExprCondition),distinct)

    //sort order by
    val sort= if(select.sort==null) having  else Sort(SortOrderParser(select.sort,expressions), having)

    //limit (该逻辑计划不支持 offset的情况，即limit 10,20语法)
    val limit=if(select.limitExpr!=null) Limit(H2ExpressionParser(select.limitExpr),sort) else sort

    return limit
  }

}
