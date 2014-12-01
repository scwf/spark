package org.mytest;

import java.sql.SQLException;

import org.h2.adapter.sqlparse.H2SqlParserAdapter;
import org.h2.command.Command;
import org.junit.Before;
import org.junit.Test;

public class MyTest3
{
    
    H2SqlParserAdapter h2SqlParser=new H2SqlParserAdapter();
    
    @Before
    public void setUp() throws Exception
    {
        h2SqlParser.setModel("Oracle");
        h2SqlParser.initH2TableSchemaMapForTest();
    }
    
    @Test
    public void test1() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select e.name from test e where e.name='aaa'");
        System.out.println(command);
    }
    
    @Test
    public void test2() throws SQLException
    {
        String sql="SELECT e.name,e.age,e.depno,d.depname FROM emp e, dep d where e.depno=d.depno and e.age>15  and e.depno=1";
        Command command=h2SqlParser.getPreparedCommand(sql);
        System.out.println(command);
    }
    
    @Test
    public void test3() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("SELECT e.name,e.age,e.depno FROM (select * from (select * from emp) ee where ee.age>20) e where e.age>0");
        System.out.println(command);
    }
    
    @Test
    public void test4() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp e left join dep d on e.depno=d.depno where e.name='aaa'");
        System.out.println(command);
    }
    
    @Test
    public void test5() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp e right join dep d on e.depno=d.depno where e.name='aaa'");
        System.out.println(command);
    }
    
    @Test
    public void test51() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp e inner join dep d on e.depno=d.depno where e.name='aaa'");
        System.out.println(command);
    }
    
    @Test
    public void test6() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("SELECT * from test");
        System.out.println(command);
    }
    
    @Test
    public void test7() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("CREATE TABLE IF NOT EXISTS emp(name varchar(20), age int,depno int)");
        System.out.println(command);
    }
    
    @Test
    public void test8() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("CREATE TABLE bar as ( SELECT age,name FROM emp where name='aaa')");
        System.out.println(command);
    }
    
    @Test
    public void test9() throws SQLException
    {
//        Command command=h2SqlParser.getPreparedCommand("SELECT depno, name into dep from emp");
//        System.out.println(command);
    }
    
    @Test
    public void test10() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("Insert into emp(name,age)  (select depname,depno from dep)");
        System.out.println(command);
    }
    
    @Test
    public void test11() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select abs(age) from emp");
        System.out.println(command);
    }
    


}
