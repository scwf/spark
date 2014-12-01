package org.apache.spark.sql.hive.h2

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{And, EqualTo, GreaterThan, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.catalyst.types.IntegerType
import org.h2.adapter.sqlparse.H2SqlParserAdapter
import org.h2.command.CommandContainer
import org.h2.command.dml.Select

import scala.collection.mutable.ListBuffer

/**
 * Created by w00297350 on 2014/11/27.
 */
  //wuwei add
  class  H2SqlParser
  {
    def apply(input: String): LogicalPlan = {
      println("parser h2 sql and generate the logic plan")

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

        val age=UnresolvedAttribute("AGE")
        val ageFl= GreaterThan(age, Literal(1, IntegerType))

        val name=UnresolvedAttribute("NAME")
        val nameFl=EqualTo(name,Literal("bbb",StringType))

        val and=And(ageFl, nameFl)

        val filter=Filter(and, relation)

        return Project(exprArray.toSeq,filter)
      }

      //    val tableName="emp"
      //    val relation = UnresolvedRelation(None, tableName, None);
      //    val nameAttr=UnresolvedAttribute("name")
      //    val ageAttr=UnresolvedAttribute("age")
      //    val expressions=Seq(nameAttr,ageAttr)
      //    Project(expressions,relation)
      null
    }
  }

