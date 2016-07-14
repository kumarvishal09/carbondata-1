/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.integration.spark.testsuite.complexType

import java.io.File

import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.apache.spark.sql.Row
import org.carbondata.core.carbon.CarbonTableIdentifier
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension
import org.scalatest.BeforeAndAfterAll

/**
 * Test class of creating and loading for carbon table with double
 *
 */
class TestComplexTypeQuery extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {
     sql("drop table if exists complexcarbontable").show
     sql("drop table if exists complexhivetable").show
     sql("create table complexcarbontable(deviceInformationId int, channelsId string, ROMSize string, ROMName String, purchasedate string, mobile struct<imei:string, imsi:string>, MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId double,contractNumber double)  STORED BY 'org.apache.carbondata.format'  TBLPROPERTIES ('DICTIONARY_INCLUDE'='deviceInformationId', 'DICTIONARY_EXCLUDE'='channelsId','COLUMN_GROUP'='(ROMSize,ROMName)')");
     sql("LOAD DATA local inpath './src/test/resources/complextypesample.csv' INTO table complexcarbontable  OPTIONS('DELIMITER'=',', 'QUOTECHAR'='\"', 'FILEHEADER'='deviceInformationId,channelsId,ROMSize,ROMName,purchasedate,mobile,MAC,locationinfo,proddate,gamePointId,contractNumber', 'COMPLEX_DELIMITER_LEVEL_1'='$', 'COMPLEX_DELIMITER_LEVEL_2'=':')");
     sql("create table complexhivetable(deviceInformationId int, channelsId string, ROMSize string, ROMName String, purchasedate string, mobile struct<imei:string, imsi:string>, MAC array<string>, locationinfo array<struct<ActiveAreaId:int, ActiveCountry:string, ActiveProvince:string, Activecity:string, ActiveDistrict:string, ActiveStreet:string>>, proddate struct<productionDate:string,activeDeactivedate:array<string>>, gamePointId double,contractNumber double)row format delimited fields terminated by ',' collection items terminated by '$' map keys terminated by ':'")
     sql("LOAD DATA local inpath './src/test/resources/complextypesample.csv' INTO table complexhivetable");
  }

  test("select * from complexcarbontable") {
     checkAnswer(sql("select * from complexcarbontable"),
     sql("select * from complexhivetable"))
  }
  
  test("select mobile, proddate, deviceInformationId  from complexcarbontable") {
     checkAnswer(sql("select mobile, proddate, deviceInformationId  from complexcarbontable"),
     sql("select mobile, proddate, deviceInformationId  from complexhivetable"))
  }
  
  test("select mobile, MAC, deviceInformationId, purchasedate from complexcarbontable") {
     checkAnswer(sql("select mobile, MAC, deviceInformationId, purchasedate from complexcarbontable"),
     sql("select mobile, MAC, deviceInformationId, purchasedate from complexhivetable"))
  }
 
   test("select mobile, ROMSize, deviceInformationId from complexcarbontable") {
     checkAnswer(sql("select mobile, ROMSize, deviceInformationId from complexcarbontable"),
     sql("select mobile, ROMSize, deviceInformationId from complexhivetable"))
  }
   
   test("select locationinfo, purchasedate, deviceInformationId from complexcarbontable") {
     checkAnswer(sql("select locationinfo, purchasedate, deviceInformationId from complexcarbontable"),
     sql("select locationinfo, purchasedate, deviceInformationId from complexhivetable"))
  }
    test("select locationinfo, ROMName, purchasedate, deviceinformationId from complexcarbontable") {
     checkAnswer(sql("select locationinfo, ROMName, purchasedate, deviceinformationId from complexcarbontable"),
     sql("select locationinfo, ROMName, purchasedate, deviceinformationId from complexhivetable"))
  }
     test("select MAC from complexcarbontable where MAC[0] = 'MAC1'") {
     checkAnswer(sql("select MAC from complexcarbontable where MAC[0] = 'MAC1'"),
     sql("select MAC from complexhivetable where MAC[0] = 'MAC1'"))
  }
      test("select mobile from complexcarbontable where mobile.imei like '1AA%'") {
     checkAnswer(sql("select mobile from complexcarbontable where mobile.imei like '1AA%'"),
     sql("select mobile from complexhivetable where mobile.imei like '1AA%'"))
  }
      
      
     test("select locationinfo from complexcarbontable where locationinfo[0].ActiveAreaId > 2 AND locationinfo[0].ActiveAreaId < 7") {
     checkAnswer(sql("select locationinfo from complexcarbontable where locationinfo[0].ActiveAreaId > 2 AND locationinfo[0].ActiveAreaId < 7"),
     sql("select locationinfo from complexhivetable where locationinfo[0].ActiveAreaId > 2 AND locationinfo[0].ActiveAreaId < 7"))
  }
        test("select locationinfo from complexcarbontable where locationinfo[0].ActiveAreaId >= 2 AND locationinfo[0].ActiveAreaId <= 7") {
     checkAnswer(sql("select locationinfo from complexcarbontable where locationinfo[0].ActiveAreaId >= 2 AND locationinfo[0].ActiveAreaId <= 7"),
     sql("select locationinfo from complexhivetable where locationinfo[0].ActiveAreaId >= 2 AND locationinfo[0].ActiveAreaId <= 7"))
  }
           test("select locationinfo from complexcarbontable where (locationinfo[0].ActiveAreaId +5 )> 6 AND (locationinfo[0].ActiveAreaId+10) < 20") {
     checkAnswer(sql("select locationinfo from complexcarbontable where (locationinfo[0].ActiveAreaId +5 )> 6 AND (locationinfo[0].ActiveAreaId+10) < 20"),
     sql("select locationinfo from complexhivetable where (locationinfo[0].ActiveAreaId +5 )> 6 AND (locationinfo[0].ActiveAreaId+10) < 20"))
  }
              test("select count(mobile),channelsId from complexcarbontable group by mobile,channelsId") {
     checkAnswer(sql("select count(mobile),channelsId from complexcarbontable group by mobile,channelsId"),
     sql("select count(mobile),channelsId from complexhivetable group by mobile,channelsId"))
  }
              
                test("select count(mobile),channelsId from complexcarbontable group by mobile,channelsId order by channelsId") {
     checkAnswer(sql("select count(mobile),channelsId from complexcarbontable group by mobile,channelsId order by channelsId"),
     sql("select count(mobile),channelsId from complexhivetable group by mobile,channelsId order by channelsId"))
  }
        test("select count(mobile),channelsId from complexcarbontable group by mobile,channelsId order by channelsId limit 10") {
     checkAnswer(sql("select count(mobile),channelsId from complexcarbontable group by mobile,channelsId order by channelsId limit 10"),
     sql("select count(mobile),channelsId from complexhivetable group by mobile,channelsId order by channelsId limit 10"))
  }
           test("select count(mobile),channelsId from complexcarbontable where MAC[0] = 'MAC1'  group by mobile,channelsId order by channelsId limit 10") {
     checkAnswer(sql("select count(mobile),channelsId from complexcarbontable where MAC[0] = 'MAC1'  group by mobile,channelsId order by channelsId limit 10"),
     sql("select count(mobile),channelsId from complexhivetable where MAC[0] = 'MAC1'  group by mobile,channelsId order by channelsId limit 10"))
  }

  override def afterAll {
	  sql("drop table if exists complexcarbontable")
	  sql("drop table if exists complexhivetable")

  }
}
