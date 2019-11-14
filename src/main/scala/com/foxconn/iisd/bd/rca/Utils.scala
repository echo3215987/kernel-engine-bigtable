package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.net.URI
import java.sql.{DriverManager, ResultSet}
import java.util
import java.util.Properties

import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.hadoop.fs._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.io.Source

object Utils {

    def createStringSchema(columnNames: String) = {
        StructType(columnNames
          .split(",")
          .map(fieldName => StructField(fieldName,StringType, true)))
    }

    //gen test_value, test_item_result, test_item_result_detail column select sql
//    def genTestDetailItemSelectSQL(colName:String, item: Seq[String]) = {
//        //jsonb_build_object('CpyCopy.2^DLTVGCal', test_value->>'CpyCopy.2^DLTVGCal', 'CpyCopy.1^DLTVGCal', test_value->>'CpyCopy.2^DLTVGCal') as test_value
//        item.map(i=> "'"+i+"'," + "t2." + colName + "->>'" + i + "'").mkString("jsonb_build_object(",",",") as " + colName)
//    }
//
//    //gen where sql TODO:delete, no use
//    def genTestDetailWhereSQL(product: String, station: Seq[String], item: Seq[String]) = {
//        var whereSql = " where product = '" + product + "'"
//        whereSql = whereSql + " and (" + station.map(s=> "station_name='" + s + "'").mkString(" or ") + ")"
//        //        whereSql = whereSql + " and (" + item.map(i=> "array_length(array_positions(test_item, '"+ i +"'), 1)>0").mkString(" or ") + ")"
//        whereSql
//    }
//
    //gen where sql
    def genTestDetailWhereSQL(product: String, stationName: String) = {
        var whereSql = " where product = '" + product + "'"
        whereSql = whereSql + " and station_name='" + stationName + "'"
        whereSql
    }
}
