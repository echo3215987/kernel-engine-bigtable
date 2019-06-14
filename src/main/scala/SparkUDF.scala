package com.foxconn.iisd.bd.rca

import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.rca.XWJKernelEngine.configLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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

}
