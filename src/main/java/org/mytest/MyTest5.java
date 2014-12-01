package org.mytest;

import java.util.ArrayList;

import org.h2.adapter.sqlparse.H2SqlParserAdapter;
import org.h2.command.CommandContainer;
import org.h2.command.Prepared;
import org.h2.command.dml.Select;
import org.h2.expression.Expression;

public class MyTest5
{
    public static void main(String[] args)
    {
        H2SqlParserAdapter h2SqlParser=new H2SqlParserAdapter();
        h2SqlParser.setModel("Oracle");
        h2SqlParser.initH2TableSchemaMapForTest();
        CommandContainer command=(CommandContainer)h2SqlParser.getPreparedCommand("select name, age from emp");
        Prepared prepared=command.prepared;
        if(prepared instanceof Select)
        {
            Select select=(Select)prepared;
            ArrayList<Expression> expressions=select.getExpressions();
            for(Expression expr: expressions)
            {
                String columnName=expr.getColumnName();
                System.out.println(columnName);
            }
            String fullTableName=select.getTopTableFilter().toString();
            int nameIndex=fullTableName.lastIndexOf(".");
            String tableName=fullTableName.substring(++nameIndex);
            System.out.println(tableName);
        }
        
    }
}
