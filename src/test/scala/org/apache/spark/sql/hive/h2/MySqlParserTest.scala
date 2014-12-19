package org.apache.spark.sql.hive.h2

import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.syntactical.StandardTokenParsers

/**
 * Created by w00297350 on 2014/12/16.
 */

class MySqlParser extends StandardTokenParsers with PackratParsers
{
  lexical.delimiters ++= List(".",";","+","-","*")
  lexical.reserved ++= List("add","bb","cc")

  lazy val num = numericLit
  lazy val add :PackratParser[Int]= num~"+"~num ^^ {case n1~"+"~n2 => n1.toInt + n2.toInt}
  lazy val add2 :PackratParser[Int]= num~"add"~num ^^ {case n1~"add"~n2 => n1.toInt + n2.toInt}
  lazy val add3 :PackratParser[Any]= num ~"bb"~ident ^^ {case n1 ~_~d=>d}
  lazy val minus :PackratParser[Int]= num~"-"~num ^^ {case n1~"-"~n2 => n1.toInt - n2.toInt}
  lazy val multiply :PackratParser[Int]= num~"*"~num ^^ {case n1~"*"~n2 => n1.toInt * n2.toInt}
  lazy val pgm : PackratParser[Any] = add | minus|multiply|add2|add3

  def parse(input: String) =
    phrase(pgm)(new PackratReader(new lexical.Scanner(input))) match {
      case Success(result, _) => println("Success!"); println(result);Some(result)
      case aa@_ => println(aa);println("bla"); None
    }

}

object MySqlParserTest {
  def main(args: Array[String]) {
    //定义list，::表示添加,Nil表示list结束
    val prg = "10 bb r33"
    val sqlParser=new MySqlParser
    sqlParser.parse(prg)

  }
}
