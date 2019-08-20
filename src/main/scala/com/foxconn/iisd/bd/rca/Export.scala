package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat
import java.util.Date

import com.foxconn.iisd.bd.rca.SparkUDF.{genInfo, genItemJsonFormat, genStaionJsonFormat, transferArrayToString}
import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.Seq

object Export{

    val mariadbUtils = new MariadbUtils()
    val stationInfoColumn = configLoader.getString("dataset", "station_info_col")

    val itemInfoColumn = configLoader.getString("dataset", "item_info_col")

    val componentInfoColumn = configLoader.getString("dataset", "component_info_col")

    val stationInfo = configLoader.getString("dataset", "station_info_str")

    val itemInfo = configLoader.getString("dataset", "item_info_str")

    val componentInfo = configLoader.getString("dataset", "component_info_str")

    val datasetColumnNames = configLoader.getString("dataset", "bigtable_datatype_col")

    val datasetPath = configLoader.getString("minio_log_path", "dataset_path")

    def exportBigtableToCsv(spark: SparkSession, currentDatasetDF: DataFrame, currentDatasetStationItemDf: DataFrame,
                            id: String, testDeailResultGroupByDf: DataFrame) ={
        import spark.implicits._
        val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt

println("-----------------> export bigtable to file: " + id + ", start_time:" + new SimpleDateFormat(
  configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))


        val itemList = currentDatasetStationItemDf.withColumn("item", explode(col("item")))
          .withColumn("item_column", concat(col("station_name"), lit("@"), col("item")))
          .select("item_column").map(_.getString(0)).collect.toList

        //展開json欄位匯出csv提供客戶下載, 並將大表欄位儲存起來
        val jsonColumns = List(stationInfo, itemInfo, componentInfo)
        var testDeailResultGroupByFirstDf = testDeailResultGroupByDf
        for(jsonInfo <- jsonColumns){
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
              .withColumn(jsonInfo, regexp_replace(col(jsonInfo), "'", ""))
              .persist(StorageLevel.MEMORY_AND_DISK)
        }

        //station 與 component json固定資訊
        val jsonMap = Map(stationInfo -> stationInfoColumn, componentInfo -> componentInfoColumn)

        //展開json object, 存成csv
        var currentDatasetDf = currentDatasetDF
        var jsonColumnMapping = Map[String, String]()
        for(jsonInfo <- jsonColumns){
            val datasetColumnName = jsonInfo.replace("_info", "")
            currentDatasetDf = currentDatasetDf.withColumn(datasetColumnName, explode(col(datasetColumnName)))
            val infoList = currentDatasetDf.select(datasetColumnName).dropDuplicates().map(_.getString(0)).collect.toList
            var infoFielids = List[String]()
            if(jsonInfo.equals(itemInfo)){
                for(item <- itemList){
                    infoFielids = infoFielids :+ item
                    val jsonResult = item + "@result"
                    infoFielids = infoFielids :+ jsonResult
                    val jsonResultDetail = item + "@result_detail"
                    infoFielids = infoFielids :+ jsonResultDetail
                    jsonColumnMapping = jsonColumnMapping + (item -> jsonInfo)
                    jsonColumnMapping = jsonColumnMapping + (jsonResult -> jsonInfo)
                    jsonColumnMapping = jsonColumnMapping + (jsonResultDetail -> jsonInfo)
                }
            }

            if(jsonInfo.equals(stationInfo) || jsonInfo.equals(componentInfo)){
                for(info <- infoList){
                    val value = jsonMap.apply(jsonInfo)
                    for(attr <- value.split(",")){
                        val jsonValue = info + "@" + attr
                        infoFielids = infoFielids :+ jsonValue
                        jsonColumnMapping = jsonColumnMapping + (jsonValue -> jsonInfo)
                    }
                }
            }

            val schema = StructType(
                infoFielids
                  .map(fieldName => StructField(fieldName, StringType, true))
            )

            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(jsonInfo, from_json(col(jsonInfo), schema))
              .persist(StorageLevel.MEMORY_AND_DISK)
            for (column <- infoFielids) {
                testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(column, col(jsonInfo).getItem(column))
            }
        }

        for(jsonInfo <- jsonColumns){
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.drop(jsonInfo)
        }

        testDeailResultGroupByFirstDf.repartition(numExecutors).write.option("header", "true")
          .mode("overwrite").csv(datasetPath + "/" + id)

println("-----------------> export bigtable to file: " + id + ", end_time:" + new SimpleDateFormat(
  configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

        (jsonColumnMapping, testDeailResultGroupByFirstDf.columns)
    }
}
