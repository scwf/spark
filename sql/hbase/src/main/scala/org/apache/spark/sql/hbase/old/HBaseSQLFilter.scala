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
package org.apache.spark.sql.hbase

import java.util

import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.Filter.ReturnCode
import org.apache.hadoop.hbase.filter._
import org.apache.log4j.Logger
import DataTypeUtils._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.hbase.HBaseCatalog.Column

/**
 * HBaseSQLFilter: a set of PushDown filters for optimizing Column Pruning
 * and Row Filtering by using HBase Scan/Filter constructs
 *
 * Created by sboesch on 9/22/14.
 */
