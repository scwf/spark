package org.mytest;

import org.h2.adapter.sqlparse.H2SqlParserAdapter;
import org.h2.command.CommandContainer;
import org.h2.command.Prepared;
import org.h2.command.dml.Select;
import org.h2.expression.*;

import org.h2.message.DbException;
import org.h2.value.*;

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute;
import org.apache.spark.sql.catalyst.expressions.Literal;
import org.apache.spark.sql.catalyst.types.IntegerType;
import org.apache.spark.sql.catalyst.types.IntegerType$;



/**
 * Created by w00297350 on 2014/12/1.
 */
public class MyTest7 {
    public static void main(String[] args)
    {
        test1();
    }

    public static void test1()
    {
        H2SqlParserAdapter h2SqlParser=new H2SqlParserAdapter();
        h2SqlParser.setModel("Oracle");
        h2SqlParser.initH2TableSchemaMapForTest();
        CommandContainer command=(CommandContainer)h2SqlParser.getPreparedCommand("select name, age from emp where age>1 and name='aaa'");
        Prepared prepared=command.prepared;
        if(prepared instanceof Select)
        {
            Select select=(Select)prepared;
            Condition condition=(Condition)select.condition;
            if(condition!=null)
            {
               if(condition instanceof Comparison)
               {
                   Comparison comparsion=(Comparison)condition;
                   int compareType=comparsion.getType();
                   switch (compareType) {
                       case Comparison.IS_NULL:
                           //sql = left.getSQL() + " IS NULL";
                           break;
                       case Comparison.IS_NOT_NULL:
                           //sql = left.getSQL() + " IS NOT NULL";
                           break;
                       case Comparison.SPATIAL_INTERSECTS:
                           //sql = "INTERSECTS(" + left.getSQL() + ", " + right.getSQL() + ")";
                           break;
                       case Comparison.EQUAL:
                           Expression leftExpr=comparsion.getExpression(true);
                           Expression rightExpr=comparsion.getExpression(false);

                           ExpressionColumn column=(ExpressionColumn)leftExpr;
                           String columnName=column.getColumnName();

                           UnresolvedAttribute unresolvedAttribute=new UnresolvedAttribute(columnName);

                           if(rightExpr instanceof ValueExpression)
                           {
                               ValueExpression rightValue=(ValueExpression)rightExpr;
                               Value value=rightValue.getValue(null);
                               int valueType=value.getType();

                               switch(valueType)
                               {
                                   case Value.BYTE:
                                       //return ValueByte.get(Byte.parseByte(s.trim()));
                                   case Value.SHORT:

                                   case Value.INT:
                                       int intValue=value.getInt();
                                       Literal literal=new Literal(intValue, IntegerType$.MODULE$);

                                       break;
                                   case Value.LONG:

                                   case Value.DECIMAL:

                                   case Value.TIME:

                                   case Value.DATE:

                                   case Value.TIMESTAMP:

                                   case Value.BYTES:

                                   case Value.JAVA_OBJECT:

                                   case Value.STRING:

                                   case Value.STRING_IGNORECASE:

                                   case Value.STRING_FIXED:

                                   case Value.DOUBLE:

                                   case Value.FLOAT:

                                   case Value.CLOB:

                                   case Value.BLOB:

                                   case Value.ARRAY:

                                   case Value.RESULT_SET:

                                   case Value.UUID:

                                   case Value.GEOMETRY:

                                   default:
                                       throw DbException.throwInternalError("type=" + valueType);
                               }



                           }
                           else if(rightExpr instanceof ExpressionColumn)
                           {

                           }
                           else
                           {

                           }

                           //return "=";
                       case Comparison.EQUAL_NULL_SAFE:
                           //return "IS";
                       case Comparison.BIGGER_EQUAL:
                           //return ">=";
                       case Comparison.BIGGER:
                           //return ">";
                       case Comparison.SMALLER_EQUAL:
                           //return "<=";
                       case Comparison.SMALLER:
                           //return "<";
                       case Comparison.NOT_EQUAL:
                           //return "<>";
                       case Comparison.NOT_EQUAL_NULL_SAFE:
                           //return "IS NOT";
//                       case Comparison.SPATIAL_INTERSECTS:
//                           return "&&";
                       default:
                           throw DbException.throwInternalError("compareType=" + compareType);
                   }

               }
            }


        }
    }
}
