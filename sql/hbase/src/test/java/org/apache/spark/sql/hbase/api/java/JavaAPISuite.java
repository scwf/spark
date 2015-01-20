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

package org.apache.spark.sql.hbase.api.java;

import java.io.Serializable;

import org.apache.spark.sql.SQLConf;
import org.apache.spark.sql.hbase.HBaseSQLContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;

public class JavaAPISuite implements Serializable {
    private transient JavaSparkContext sc;
    private transient SQLContext sqlContext;

    @Before
    public void setUp() {
        sc = new JavaSparkContext("local", "JavaAPISuite");
        sqlContext = new HBaseSQLContext(sc);
    }

    @After
    public void tearDown() {
        sc.stop();
        sc = null;
    }

    @Test
    public void test1() {
//        boolean originalValue = sqlContext.getConf(SQLConf.CODEGEN_ENABLED(), "false"); //codegenEnabled
//        sqlContext.setConf(SQLConf.CODEGEN_ENABLED(), "true");
//        Row results = sqlContext.sql("SELECT col1 FROM ta GROUP BY col1").collect();
//        assert(results.size() == 14);
//        sqlContext.setConf(SQLConf.CODEGEN_ENABLED(), originalValue);
    }
}
