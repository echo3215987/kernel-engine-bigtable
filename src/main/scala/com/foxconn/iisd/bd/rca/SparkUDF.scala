package com.foxconn.iisd.bd.rca

import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.spark.sql.functions.udf
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.Seq

object SparkUDF{

    //取split最後一個element
    def getLast = udf((xs: Seq[String]) => (xs.last))
    //取得測試樓層與線體對應表
    def getFloorLine = udf {
        s: String =>
            configLoader.getString("test_floor_line", "code_"+s)
    }
    //parse array to string
    def parseArrayToString = udf {
        itemValue: Seq[String] => {
            itemValue.map {
                _.replace("\004", "^D")
                .mkString("'", "", "'")
            }
        }.mkString(",")
    }

    //parse string to json string
    def parseStringToJSONString = udf {
        itemValue: Seq[String] => {
            itemValue.map {
                ele => {
                    var newString = ""
                    var eleArray = ele.split("\003")
                    eleArray.map {
                        nEleArray => {
                            newString = newString + nEleArray.mkString("\"", "", "\"")
                            if (nEleArray.indexOf("\004") != -1) {
                               newString = newString + ":"
                            }
                        }
                        if (eleArray.size == 1) {
                            newString = newString + null
                        }
                    }
                    val newele = newString
                    newele.replace("\004", "^D")
                }
            }.mkString("{", ",", "}")
        }
    }

    //gen where sql
    def genTestDetailWhereSQL = udf {
        (product: String, station: Seq[String], item: Seq[String]) =>{
            var whereSql = " where product = '" + product + "'"
            whereSql = whereSql + " and (" + station.map(s=> "station_name='" + s + "'").mkString(" or ") + ")"
            //whereSql = whereSql + " and (" + item.map(i=> "array_length(array_positions(test_item, '"+ i +"'), 1)>0").mkString(" or ") + ")"
            whereSql
        }
    }

    //gen test item select sql
    def genTestDetailItemSelectSQL = udf {
        (colName:String, item: Seq[String]) =>{
            //test_value->'first_name', test_value->'location'
            item.map(i=> colName +"->'" + i + "'").mkString(",")
        }

    }

    //判斷是第一筆還最後一筆
    def getFirstOrLastRow = udf {
        (first: Int, last: Int) =>{
            var value = ""
            if(first == 1) value = "first"
            else
                value = "last"
            value
        }

    }

    //將station的每個欄位組成json value的方式
    def genStaionJsonFormat = udf {
        (station_name: String, station_attribute: String, station_value:String) =>{
            "\"" + station_name + "@" + station_attribute + "\":" + "\"" + station_value + "\""
        }
    }

    //merge station的相關欄位
    def genStaionInfo = udf {
        (station_info: Seq[String], station_json_value: Seq[String]) =>{
            station_info ++ station_json_value
        }
    }

    //merge item的相關欄位
    def genItemInfo = udf {
        (station_name: String, item: Seq[String], test_value:String) =>{
            val jsonMap = parse(test_value).values.asInstanceOf[Map[String, Any]]
            item.map(i=>{
                var value = jsonMap.apply(i)
                if(value != null)
                    value = "\"" + value + "\""
                "\"" + station_name + "@" + i +"\":" + value
            }).mkString(",")

        }
    }
    //轉換工站與測項 array to string
    def transferArrayToString = udf {
        (arr: Seq[String]) =>{
            "{" + arr.mkString(",")+ "}"
        }
    }
}
