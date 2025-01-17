// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.codehaus.groovy.runtime.IOGroovyMethods
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_schema_change_varchar_to_datev2") {
    def tbName = "test_schema_change_varchar_to_datev2"
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    backend_id = backendId_to_backendIP.keySet()[0]
    def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
    logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def configList = parseJson(out.trim())
    assert configList instanceof List

    sql """ DROP TABLE IF EXISTS ${tbName} FORCE"""
    // Create table and disable light weight schema change
    sql """
            CREATE TABLE IF NOT EXISTS ${tbName}
            (
                `k1` int(11) NOT NULL,
                `k2` varchar(4096) NOT NULL,
                `k3` VARCHAR(4096) NOT NULL,
                `v1` float SUM NOT NULL,
                `v2` decimal(20, 7) SUM NOT NULL
            )
            AGGREGATE KEY(`k1`,`k2`,`k3`)
            DISTRIBUTED BY HASH(`k1`,`k2`) BUCKETS 1
            PROPERTIES("replication_num" = "1", "light_schema_change" = "false");
        """
    // insert
    sql """ insert into ${tbName} values(1,"1","20200101",1,1), (2,"2","2020/01/02",2,2), (3,"3","2020-01-03",3,3), (4,"4","200104",4,4), (5,"5","20/01/05",5,5), (6,"6","20-01-06",6,6)"""
    // select
    qt_sql_1 """select * from ${tbName} ORDER BY `k1`;"""

    sql """ alter table ${tbName} modify column `k3` date; """
    int max_try_secs = 300
    Awaitility.await().atMost(max_try_secs, TimeUnit.SECONDS).with().pollDelay(100, TimeUnit.MILLISECONDS).await().until(() -> {
        String result = getJobState(tbName)
        if (result == "FINISHED") {
            return true;
        }
        return false;
    });


    sql """sync"""
    qt_sql_2 """select * from ${tbName} ORDER BY `k1`;"""
    trigger_and_wait_compaction(tbName, "cumulative")
    sql """sync"""
    qt_sql_3 """select * from ${tbName} ORDER BY `k1`;"""
    sql """delete from ${tbName} where `k3` = '2020-01-02';"""
    sql """sync"""
    qt_sql_4 """select * from ${tbName} ORDER BY `k1`;"""

    sql """ DROP TABLE  ${tbName} force"""
}
