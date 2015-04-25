/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst

import scala.language.implicitConversions
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.parsing.input.CharArrayReader.EofCh

import org.apache.spark.sql.catalyst.plans.logical._

private[sql] object KeywordNormalizer {
  def apply(str: String): String = str.toLowerCase()
}

private[sql] abstract class AbstractSparkSQLParser
  extends StandardTokenParsers with PackratParsers {

  def parse(input: String): LogicalPlan = {
    // Initialize the Keywords.
    lexical.initialize(reservedWords)
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError => sys.error(failureOrError.toString)
    }
  }

  protected case class Keyword(str: String) {
    def normalize: String = KeywordNormalizer(str)
    def parser: Parser[String] = normalize
  }

  protected implicit def asParser(k: Keyword): Parser[String] = k.parser

  // By default, use Reflection to find the reserved words defined in the sub class.
  // NOTICE, Since the Keyword properties defined by sub class, we couldn't call this
  // method during the parent class instantiation, because the sub class instance
  // isn't created yet.
  // 貌似这里 用lazy还是有玄机的，不想在初始化时父类里面调用
  // 但是说实在的，定义一个 Keyword 类，采用反射获取 reserved 很奇怪， 何不直接定义一个Seq【String】
  protected lazy val reservedWords: Seq[String] =
    this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].normalize)

  // Set the keywords as empty by default, will change that later.
  override val lexical = new SqlLexical

  protected def start: Parser[LogicalPlan]

  // Returns the whole input string
  protected lazy val wholeInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(in.source.toString, in.drop(in.source.length()))
  }

  // Returns the rest of the input string that are not parsed yet
  protected lazy val restInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(
        in.source.subSequence(in.offset, in.source.length()).toString,
        in.drop(in.source.length()))
  }
}

/**
 *  spark sql 提供的词法解析器， 该解析器的主要解析出 keywords， identifier， numeric， string 和 delimiters
 *  为了区分identifiers 和 keywords, 词法解析器根据reserved集合来匹配keywords，只要是在reserved里面的字符串都不会被解析为identifire 而是会被解析成 keyword
 *  用户可以自定义reserved
 *  另外分隔符可以通过 delimiters 来设置
 *
 *  通常来讲，这个词法解析器用于将字符输入切割成多个token，然后我们会将这些token 传给一个token parser去解析
 *  [[scala.util.parsing.combinator.syntactical.TokenParsers]].)
 */
class SqlLexical extends StdLexical {
  // 定义了一个 float 的token，后面解析float字符串用
  case class FloatLit(chars: String) extends Token {
    override def toString: String = chars
  }

  /* This is a work around to support the lazy setting */
  def initialize(keywords: Seq[String]): Unit = {
    reserved.clear()
    reserved ++= keywords
  }

  delimiters += (
    "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
    ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~", "<=>"
  )

  protected override def processIdent(name: String) = {
    // 全部转为小写
    val token = KeywordNormalizer(name)
    // 这个够直观了吧
    if (reserved contains token) Keyword(token) else Identifier(name)
  }

  override lazy val token: Parser[Token] =
    ( identChar ~ (identChar | digit).* ^^
      { case first ~ rest => processIdent((first :: rest).mkString) }
    | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
        case i ~ None    => NumericLit(i.mkString)
        case i ~ Some(d) => FloatLit(i.mkString + "." + d.mkString)
      }
    | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^
      { case chars => StringLit(chars mkString "") }
    | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^
      { case chars => StringLit(chars mkString "") }
    | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^
      { case chars => Identifier(chars mkString "") }
    | EofCh ^^^ EOF
    | '\'' ~> failure("unclosed string literal")
    | '"' ~> failure("unclosed string literal")
    | delim
    | failure("illegal character")
    )

  override def identChar: Parser[Elem] = letter | elem('_')

  override def whitespace: Parser[Any] =
    ( whitespaceChar
    | '/' ~ '*' ~ comment
    | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
    | '#' ~ chrExcept(EofCh, '\n').*
    | '-' ~ '-' ~ chrExcept(EofCh, '\n').*
    | '/' ~ '*' ~ failure("unclosed comment")
    ).*
}

