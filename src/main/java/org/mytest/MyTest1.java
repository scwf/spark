package org.mytest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MyTest1
{
    static String url = "jdbc:h2:tcp://localhost:9092/./data/baseDir/mydb";

    public static void main(String[] args) throws Exception
    {
        Class.forName("org.h2.Driver");
        Connection conn = DriverManager.getConnection(
                url, "sa", "");
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE IF EXISTS my_table");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS my_table(name varchar(20))");
        stmt.executeUpdate("INSERT INTO my_table(name) VALUES('zhh')");

        ResultSet rs = stmt.executeQuery("SELECT name FROM my_table");
        rs.next();
        System.out.println(rs.getString(1));

        stmt.close();
        conn.close();

    }

}
