package org.h2.adapter.sqlparse;

import java.util.ArrayList;
import java.util.HashMap;

import org.h2.engine.ConnectionInfo;
import org.h2.engine.Database;
import org.h2.engine.Mode;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.mvstore.db.MVTable;
import org.h2.table.Column;
import org.h2.table.Table;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.command.Command;
import org.h2.command.ddl.CreateTableData;

public class H2SqlParserAdapter
{
    private ConnectionInfo ci = new ConnectionInfo("mem:sqlParser");

    private Database db = new Database(ci);

    private User user = new User(db, 0, "sqlParser", false);

    private Session session = new Session(db, user, 0);

    private HashMap<String, Table> tableSchemaMap = New.hashMap();
    
    
    public void setModel(String mode)
    {
        db.setMode(Mode.getInstance(mode));
    }

    public HashMap<String, Table> initH2TableSchemaMapForTest()
    {
        CreateTableData data = new CreateTableData();
        data.schema = db.getSchema("PUBLIC");
        ArrayList<Column> cols = data.columns;
        cols.add(new Column("NAME", Value.STRING));
        cols.add(new Column("AGE", Value.INT));
        data.tableName = "TEST";
        data.id = 0;
        MVTable mytable = new MVTable(data);
        mytable.myinit(session);
        tableSchemaMap.put("TEST", mytable);
        
        data=new CreateTableData();
        data.schema=db.getSchema("PUBLIC");
        cols = data.columns;
        cols.add(new Column("NAME", Value.STRING));
        cols.add(new Column("AGE", Value.INT));
        cols.add(new Column("DEPNO", Value.INT));
        data.tableName = "EMP";
        data.id = 1;
        mytable =new MVTable(data);
        mytable.myinit(session);
        tableSchemaMap.put("EMP", mytable);
        
        data=new CreateTableData();
        data.schema=db.getSchema("PUBLIC");
        cols = data.columns;
        cols.add(new Column("DEPNO", Value.INT));
        cols.add(new Column("DEPNAME", Value.STRING));
        data.tableName = "DEP";
        data.id = 2;
        mytable =new MVTable(data);
        mytable.myinit(session);
        tableSchemaMap.put("DEP", mytable);

        return tableSchemaMap;
    }

    public Command getPreparedCommand(String sql)
    {
        session.setTableSchemaMap(tableSchemaMap);
        Command command = session.prepareLocal(sql);
        return command;
    }
    
}
