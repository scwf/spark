package org.mytest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

public class MyServer
{
    
    protected Properties prop = new Properties();
    protected Connection conn;
    protected Statement stmt;
    protected PreparedStatement ps;
    protected ResultSet rs;
    protected String sql;
    protected String url;
    protected boolean dbCloseDelay = true;
    
    private void initDefaults() throws Exception {
        System.setProperty("h2.lobInDatabase", "false");
        System.setProperty("h2.lobClientMaxSizeMemory", "1024");
        System.setProperty("java.io.tmpdir", "data/tmp");
        System.setProperty("h2.baseDir", "data/baseDir");
        ArrayList<String> list = new ArrayList<String>();
        list.add("-tcp");
        org.h2.tools.Server.main(list.toArray(new String[list.size()]));
    }
    

    public static void main(String[] args) throws Exception
    {
        
        MyServer test1=new MyServer();
        test1.initDefaults();
    }

}
