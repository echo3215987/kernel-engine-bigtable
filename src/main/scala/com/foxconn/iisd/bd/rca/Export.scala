package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat
import java.util.Date

import com.foxconn.iisd.bd.rca.SparkUDF.{genInfo, genItemJsonFormat, genStaionJsonFormat, transferArrayToString}
import com.foxconn.iisd.bd.rca.XWJBigtable.{configContext, configLoader}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.Seq

object Export{

    def exportBigtableToCsv(configContext: ConfigContext, currentDatasetDF: DataFrame, currentDatasetStationItemDf: DataFrame,
                            id: String, testDeailResultGroupByDf: DataFrame) ={
      val sparkSession = configContext.sparkSession
      import sparkSession.implicits._

println("-----------------> export bigtable to file: " + id + ", start_time:" + new SimpleDateFormat(
        configContext.jobDateFmt).format(new Date().getTime()))

        val itemList = currentDatasetStationItemDf.withColumn("item", explode(col("item")))
          .withColumn("item_column", concat(col("station_name"), lit("@"), col("item")))
          .select("item_column").map(_.getString(0)).collect.toList

        //展開json欄位匯出csv提供客戶下載, 並將大表欄位儲存起來
        val jsonColumns = List(configContext.stationInfo, configContext.itemInfo, configContext.componentInfo)
        var testDeailResultGroupByFirstDf = testDeailResultGroupByDf
        for(jsonInfo <- jsonColumns){
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
              .withColumn(jsonInfo, regexp_replace(col(jsonInfo), "'", ""))
              .persist(StorageLevel.MEMORY_AND_DISK_SER)
        }

        //station 與 component json固定資訊
        val jsonMap = Map(configContext.stationInfo -> configContext.stationInfoColumns, configContext.componentInfo -> configContext.componentInfoColumns)

        //展開json object, 存成csv
        var currentDatasetDf = currentDatasetDF
        var jsonColumnMapping = Map[String, String]()
        for(jsonInfo <- jsonColumns){
            val datasetColumnName = jsonInfo.replace("_info", "")
            currentDatasetDf = currentDatasetDf.withColumn(datasetColumnName, explode(col(datasetColumnName)))
            val infoList = currentDatasetDf.select(datasetColumnName).dropDuplicates().map(_.getString(0)).collect.toList
            var infoFielids = List[String]()
            if(jsonInfo.equals(configContext.itemInfo)){
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

            if(jsonInfo.equals(configContext.stationInfo) || jsonInfo.equals(configContext.componentInfo)){
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
              .persist(StorageLevel.MEMORY_AND_DISK_SER)
            for (column <- infoFielids) {
                testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(column, col(jsonInfo).getItem(column))
            }
        }

        for(jsonInfo <- jsonColumns){
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.drop(jsonInfo)
        }

        testDeailResultGroupByFirstDf.repartition(configContext.sparkNumExcutors).write.option("header", "true")
          .mode("overwrite").csv(configContext.datasetPath + "/" + id)

println("-----------------> export bigtable to file: " + id + ", end_time:" + new SimpleDateFormat(
        configContext.jobDateFmt).format(new Date().getTime()))

        (jsonColumnMapping, testDeailResultGroupByFirstDf.columns)
    }
}
