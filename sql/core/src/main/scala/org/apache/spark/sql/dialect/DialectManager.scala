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

package org.apache.spark.sql.dialect

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.JavaConversions._

/**
 * Dialect Information
 */
case class DialectDesc(name: String, dialect: Dialect)

abstract class DialectManager(context: SQLContext) {

  def parse(sql: String): LogicalPlan

  def buildDataFrame(sql: String): DataFrame = {
    DataFrame(context, parse(sql))
  }

  def switchDialect(dialect: String)

  def getCurrentDialect: DialectDesc

  def getAvailableDialects: Map[String, Dialect]

  def registerDialect(name: String, dialect: Dialect)

  def registerDialect(name: String, fullClassName: String)

  def dropDialect(name: String)
}

class DefaultDialectManager(context: SQLContext)
  extends DialectManager(context) with Logging {

  val dialects = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, Dialect]())

  @volatile
  var curDialect = context.conf.dialect

  // add sql parser
  dialects.put(SparkSqlDialect.name, SparkSqlDialect)
  loadDefaultDialects()

  /**
   * load default dialects from `SQLConf`
   */
  private def loadDefaultDialects(): Unit = {
    context.conf.getAllConfs.foreach({
      case (key, value) if key.startsWith(DefaultDialectManager.CONF_MARKER) =>
        registerDialect(key.drop(DefaultDialectManager.CONF_MARKER.length + 1), value)
      case _ =>
    })
  }

  override def switchDialect(dialect: String): Unit = {
    logInfo(s"Switching dialect from ${getCurrentDialect.name} to $dialect")
    curDialect = dialect
  }

  override def getCurrentDialect = {
    Option(dialects.get(curDialect)).map(DialectDesc(curDialect, _))
      .getOrElse(sys.error(s"The dialect $curDialect dose not exist!"))
  }

  override def getAvailableDialects = dialects.toMap

  override def registerDialect(name: String, dialect: Dialect): Unit = {
    if (dialects.containsKey(name)) {
      sys.error(s"The dialect $name already exists,try another name!")
    }

    logInfo(s"Registering dialect with name $name")
    dialects.put(name, dialect)
  }

  override def registerDialect(name: String, fullClassName: String): Unit = {
    try {
      // TODO: support  singleton object
      val clazz = Class.forName(fullClassName)
      val dialect = clazz.newInstance().asInstanceOf[Dialect]

      registerDialect(name, dialect)
    } catch {
      case _: ClassNotFoundException =>
        sys.error(s"Dialect class $fullClassName not found!")
      case _: ClassCastException =>
        sys.error(s"Class $fullClassName is not a subclass of " +
          s"org.apache.spark.sql.dialect.Dialect")
    }
  }

  override def dropDialect(name: String) = {
    if (curDialect == name) {
      sys.error(s"Can not drop a dialect that is being used!")
    }

    if (dialects.containsKey(name)) {
      dialects.remove(name)
      logInfo(s"Drop dialect $name successfully")
    } else {
      logWarning(s"Drop dialect failed,because dialect $name dose not exist!")
    }
  }

  override def parse(sql: String): LogicalPlan = {
    // check if the input sql is dialect command first
    DialectCommandParser(sql, false).getOrElse(
      getCurrentDialect.dialect.parse(sql)
    )
  }
}

object DefaultDialectManager {
  val CONF_MARKER = "spark.sql.dialects"
}

