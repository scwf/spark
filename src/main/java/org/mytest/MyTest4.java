package org.mytest;

import java.sql.SQLException;

import org.h2.adapter.sqlparse.H2SqlParserAdapter;
import org.h2.command.Command;
import org.junit.Before;
import org.junit.Test;

public class MyTest4
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
        Command command=h2SqlParser.getPreparedCommand("select * from test e where e.name='aaa'");
        System.out.println(command);
    }
    
    @Test
    public void test2() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp where not(name is null)");
        System.out.println(command);
    }
    
    @Test
    public void test3() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp where not (age between 10 and 20)");
        System.out.println(command);
    }
    
    @Test
    public void test4() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp where age in (10,20)");
        System.out.println(command);
    }
    
    @Test
    public void test5() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp where name like '%d'");
        System.out.println(command);
    }
   
    @Test
    public void test6() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp e where exists (select * from dep d where e.depno=d.depno)");
        System.out.println(command);
    }
    
    @Test
    public void test7() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("SELECT e.name,e.age,e.depno,d.depname FROM emp e, dep d where e.depno=d.depno and e.age>15  and e.depno=1");
        System.out.println(command);
    }
    
    @Test
    public void test9() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select depno from emp union select depno from dep union select depno from dep");
        System.out.println(command);
    }
    
    @Test
    public void test10() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select depno from emp union all select depno from dep");
        System.out.println(command);
    }
    
    @Test
    public void test11() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select depno from emp intersect select depno from dep");
        System.out.println(command);
    }
    
    @Test
    public void test12() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select depno from emp minus select depno from dep");
        System.out.println(command);
    }
    
    @Test
    public void test13() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp e left join dep d on e.depno=d.depno where e.name='aaa'");
        System.out.println(command);
    }
    
    @Test
    public void test14() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp e right join dep d on e.depno=d.depno where e.name='aaa'");
        System.out.println(command);
    }
    
    @Test
    public void test15() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp e inner join dep d on e.depno=d.depno where e.name='aaa'");
        System.out.println(command);
    }
    
    @Test
    public void test16() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("SELECT e.name,e.age,e.depno FROM (select * from (select * from emp) ee where ee.age>20) e where e.age>0");
        System.out.println(command);
    }
    
    @Test
    public void test17() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select case age when 1 then 11 when 2 then 22 else 33 end as age2 from emp");
        System.out.println(command);
    }
    
    @Test
    public void test18() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("CREATE TABLE IF NOT EXISTS emp(name varchar(20), age int,depno int)");
        System.out.println(command);
    }
    
    @Test
    public void test19() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("CREATE TABLE bar as ( SELECT age,name FROM emp where name='aaa')");
        System.out.println(command);
    }
    
    @Test
    public void test20() throws SQLException
    {
//        Command command=h2SqlParser.getPreparedCommand("SELECT depno, name into dep from emp");
//        System.out.println(command);
    }
    
    @Test
    public void test21() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("Insert into emp(name,age)  (select depname,depno from dep)");
        System.out.println(command);
    }
    
    @Test
    public void test22() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select abs(age)+min(age) from emp");
        System.out.println(command);
    }
    
    @Test
    public void test24() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp a, (select * from emp where name='aaa') b where a.age=b.age");
        System.out.println(command);
    }
    
    @Test
    public void test25() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from (SELECT e.name,e.age,e.depno FROM (select * from (select name,age,ea.depno from emp ea left join dep ed on ea.depno=ed.depno) ee where ee.age>20) e where e.age>0) t1, emp t2 where t1.name=t2.name");
        System.out.println(command);
    }
    
    @Test
    public void test26() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select * from emp a, (select * from emp) b, dep c where a.depno=c.depno and b.depno=c.depno and a.name=b.name");
        System.out.println(command);
    }
    
    @Test
    public void test27() throws SQLException
    {
        Command command=h2SqlParser.getPreparedCommand("select name, age from emp");
        System.out.println(command);
    }




}
