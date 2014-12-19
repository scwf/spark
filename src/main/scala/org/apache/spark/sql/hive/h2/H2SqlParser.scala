package org.apache.spark.sql.hive.h2

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.IntegerType
import org.apache.spark.sql.hive.h2.expression._
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

    //parse the sample code :select name, age from emp
    def sql1(input:String):LogicalPlan=
    {
       val tableName="emp"
       val relation = UnresolvedRelation(None, tableName, None);
       val tbl_dep="dep"
       val relation_dep=UnresolvedRelation(None, tbl_dep,None)
       val rel=Join(relation,relation_dep,Inner,None)
       val relation_dep2=UnresolvedRelation(None,"dep",None)
       val rel2=Join(rel,relation_dep2,Inner,None)

       val nameAttr=UnresolvedAttribute("name")
       val ageAttr=UnresolvedAttribute("age")
       val expressions=Seq(nameAttr,ageAttr)
       val project=Project(expressions,rel2)
       val seqOrder:Seq[SortOrder]=
       {
        SortOrder(ageAttr, Descending)::Nil
       }
       val sort=Sort(seqOrder,project);
       sort
    }

    //parse integrate
    def sql3(input:String):LogicalPlan=
    {
      val h2SqlParser=new H2SqlParserAdapter();
      h2SqlParser.setModel("Oracle");
      h2SqlParser.initH2TableSchemaMapForTest();
      val command: CommandContainer =h2SqlParser.getPreparedCommand(input).asInstanceOf[CommandContainer]
      val prepared=command.prepared;

      prepared match
      {
        case select:Select =>
          SelectParser(select)
        case _ =>
          sys.error("UNSupported the h2 sql grammar parser!")
      }

    }



  }

