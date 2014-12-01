package org.mytest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.h2.command.Command;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Database;
import org.h2.engine.Engine;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Mytest2
{
    static String url = "jdbc:h2:tcp://localhost:9092/./data/baseDir/mydb";

    Statement stmt;

    Connection conn;

    @Before
    public void setUp() throws Exception
    {
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection(url, "sa", "");
        stmt = conn.createStatement();
    }

    @After
    public void tearDown() throws Exception
    {
        stmt.close();
        conn.close();
    }

    @Test
    public void test() throws SQLException
    {
        stmt.executeUpdate("DROP TABLE IF EXISTS emp");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS emp(name varchar(20), age int,depno int)");
        stmt.executeUpdate("INSERT INTO emp(name,age,depno) VALUES('aaa',20,1)");
        stmt.executeUpdate("INSERT INTO emp(name,age,depno) VALUES('bbb',30,1)");

        stmt.executeUpdate("DROP TABLE IF EXISTS dep");
        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS dep(depno int, depname varchar(20))");
        stmt.executeUpdate("INSERT INTO dep(depno,depname) VALUES(1,'dep1')");

        ResultSet rs = stmt
                .executeQuery("SELECT e.name,e.age,e.depno,d.depname FROM emp e, dep d where e.depno=d.depno and e.age>15");
        while (rs.next())
        {
            System.out.println(rs.getString(1) + "\t" + rs.getInt(2) + "\t"
                    + rs.getInt(3) + "\t" + rs.getString(4));
        }
    }

    @Test
    public void test2() throws SQLException
    {
        ResultSet rs = stmt
                .executeQuery("SELECT e.name,e.age,e.depno,d.depname FROM emp e, dep d where e.depno=d.depno and e.age>15 and e.depno=1");
        while (rs.next())
        {
            System.out.println(rs.getString(1) + "\t" + rs.getInt(2) + "\t"
                    + rs.getInt(3) + "\t" + rs.getString(4));
        }
    }

    @Test
    public void test3() throws SQLException
    {
        ResultSet rs = stmt
                .executeQuery("SELECT e.name,e.age,e.depno FROM (select * from emp where age>20) e");
        while (rs.next())
        {
            System.out.println(rs.getString(1) + "\t" + rs.getInt(2) + "\t"
                    + rs.getInt(3));
        }
    }

    @Test
    public void test4() throws SQLException
    {
        ResultSet rs = stmt
                .executeQuery("SELECT e.name,e.age,e.depno FROM (select * from (select * from emp) ee where ee.age>20) e where e.age>0");
        while (rs.next())
        {
            System.out.println(rs.getString(1) + "\t" + rs.getInt(2) + "\t"
                    + rs.getInt(3));
        }
    }

    @Test
    public void test5() throws SQLException
    {
        ResultSet rs = stmt.executeQuery("select * from emp");
        while (rs.next())
        {
            System.out.println(rs.getString(1) + "\t" + rs.getInt(2) + "\t"
                    + rs.getInt(3));
        }
    }

    @Test
    public void test6() throws SQLException
    {
        ResultSet rs = stmt
                .executeQuery("select * from emp, dep where emp.depno=dep.depno");
        while (rs.next())
        {
            System.out.println(rs.getString(1) + "\t" + rs.getInt(2) + "\t"
                    + rs.getInt(3));
        }
    }

    @Test
    public void testN() throws SQLException
    {
        // ConnectionInfo ci = new ConnectionInfo("mem:mydb");
        // ci.setUserName("sa");
        Session session = new Session(null, null, 0);
        Command command = session.prepareLocal("select * from test");

    }

}
