package org.mytest;

import org.h2.adapter.sqlparse.H2SqlParserAdapter;
import org.h2.command.CommandContainer;
import org.h2.command.Prepared;
import org.h2.command.dml.Select;
import org.h2.expression.Expression;

import java.util.ArrayList;

/**
 * Created by w00297350 on 2014/11/29.
 */
public class MyTest6 {
    public static void main(String[] args)
    {
        H2SqlParserAdapter h2SqlParser=new H2SqlParserAdapter();
        h2SqlParser.setModel("Oracle");
        h2SqlParser.initH2TableSchemaMapForTest();
        CommandContainer command=(CommandContainer)h2SqlParser.getPreparedCommand("CALL aa(1)");
        Prepared prepared=command.prepared;


    }

}
