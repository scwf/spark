package org.apache.spark.sql.hive.h2

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.hive.h2.expression.{H2ExpressionParser, SortOrderParser, ConditionAndOrParser, ComparisonParser}
import org.h2.adapter.sqlparse.H2SqlParserAdapter
import org.h2.command.CommandContainer
import org.h2.command.dml.Select
import org.h2.expression.{ConditionAndOr, Comparison, Condition}

import scala.collection.mutable.ListBuffer

/**
 * Created by w00297350 on 2014/11/27.
 */
  //wuwei add
  class  H2SqlParser
  {
    def apply(input: String): LogicalPlan = {
      println("parser h2 sql and generate the logic plan")

      return sql3(input)

    }

    //copy from catalyst Sql Parser
    protected def assignAliases(exprs: Seq[Expression]): Seq[NamedExpression] = {
      exprs.zipWithIndex.map {
        case (ne: NamedExpression, _) => ne
        case (e, i) => Alias(e, s"c$i")()
      }
    }

    //parse the sample code :select name, age from emp
    def sql1(input:String):LogicalPlan=
    {
       val tableName="emp"
       val relation = UnresolvedRelation(None, tableName, None);
       val nameAttr=UnresolvedAttribute("name")
       val ageAttr=UnresolvedAttribute("age")
       val expressions=Seq(nameAttr,ageAttr)
       val project=Project(expressions,relation)
       val seqOrder:Seq[SortOrder]=
       {
         SortOrder(ageAttr, Descending)::Nil
       }
       val sort=Sort(seqOrder,project);
       sort
    }

    //parse where
    def sql2(input:String):LogicalPlan=
    {
      val h2SqlParser=new H2SqlParserAdapter();
      h2SqlParser.setModel("Oracle");
      h2SqlParser.initH2TableSchemaMapForTest();
      val command: CommandContainer =h2SqlParser.getPreparedCommand(input).asInstanceOf[CommandContainer]
      val prepared=command.prepared;

      if(prepared.isInstanceOf[Select])
      {
        val select=prepared.asInstanceOf[Select];
        val expressions=select.getExpressions();

        val exprArray=new ListBuffer[UnresolvedAttribute]
        for(i <- 0 to expressions.size()-1)
        {
          val expr=expressions.get(i);
          val columnName=expr.getColumnName();
          exprArray.append(UnresolvedAttribute(columnName))
        }

        val fullTableName=select.getTopTableFilter().toString();
        val nameIndex=fullTableName.lastIndexOf(".")+1;
        val tableName=fullTableName.substring(nameIndex);

        val relation = UnresolvedRelation(None, tableName, None);

        //parse where

        val age=UnresolvedAttribute("AGE")
        val ageFl= GreaterThan(age, Literal(1, IntegerType))

        val name=UnresolvedAttribute("NAME")
        val nameFl=EqualTo(name,Literal("bbb",StringType))

        val and=And(ageFl, nameFl)

        val filter=Filter(and, relation)

        return Project(exprArray.toSeq,filter)
      }
      null;
    }

    //parse test ComparisonParser
    def sql3(input:String):LogicalPlan=
    {
      val h2SqlParser=new H2SqlParserAdapter();
      h2SqlParser.setModel("Oracle");
      h2SqlParser.initH2TableSchemaMapForTest();
      val command: CommandContainer =h2SqlParser.getPreparedCommand(input).asInstanceOf[CommandContainer]
      val prepared=command.prepared;

      if(prepared.isInstanceOf[Select])
      {
        val select=prepared.asInstanceOf[Select];

        val fullTableName=select.getTopTableFilter().toString();
        val nameIndex=fullTableName.lastIndexOf(".")+1;
        val tableName=fullTableName.substring(nameIndex);

        // relation
        val relation = UnresolvedRelation(None, tableName, None);

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

        //having
        val havingExprCondition=select.havingExprForSpark
        val having= if(havingExprCondition==null) project else Filter(H2ExpressionParser(havingExprCondition),project)

        //sort order by
        val sort= if(select.sort==null) project  else Sort(SortOrderParser(select.sort,expressions), having)

        return sort
      }
      else
      {
        sys.error("UNSupported the h2 sql grammar parser!")
      }

    }



  }

