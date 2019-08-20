package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.Locale
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{regexp_extract, regexp_replace, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.foxconn.iisd.bd.rca.SparkUDF._
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel


object XWJBigtable {

  var configLoader = new ConfigLoader()
  val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

  def main(args: Array[String]): Unit = {

    val limit = 1
    var count = 0

    println("xwj-bigtable-v1:")

    while (count < limit) {
      println(s"count: $count")

      try {
        configLoader.setDefaultConfigPath("""conf/default.yaml""")
        if (args.length == 1) {
          configLoader.setDefaultConfigPath(args(0))
        }
        XWJBigtable.start()
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }

      count = count + 1

      Thread.sleep(5000)
    }

  }

  def start(): Unit = {


    var date = new Date()
    val flag = date.getTime().toString
    val jobStartTime: String = new SimpleDateFormat(
      configLoader.getString("summary_log_path", "job_fmt")).format(date.getTime())
    println("job start time : " + jobStartTime)
    val executeTime: String = new SimpleDateFormat(
      configLoader.getString("log_prop", "product_dt_fmt")).format(date.getTime())
    println("execute time : " + executeTime)
    val nextExcuteDate = org.apache.commons.lang.time.DateUtils.addMinutes(date, 60)
    val nextExcuteTime = new SimpleDateFormat(
      configLoader.getString("summary_log_path", "job_fmt")).format(nextExcuteDate.getTime())
    println("next execute time : " + nextExcuteTime)
    //    Summary.setJobStartTime(jobStartTime)

    println(s"flag: $flag" + ": xwj")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkBuilder = SparkSession
      .builder
      .appName(configLoader.getString("spark", "job_name"))
      .master(configLoader.getString("spark", "master"))

    val confStr = configLoader.getString("spark", "conf")

    val confAry = confStr.split(";").map(_.trim)
    for (i <- 0 until confAry.length) {
      val configKeyValue = confAry(i).split("=").map(_.trim)
      println("conf ===> " + configKeyValue(0) + " : " + configKeyValue(1))
      sparkBuilder.config(configKeyValue(0), configKeyValue(1))
    }

    val spark = sparkBuilder.getOrCreate()

    val configMap = spark.conf.getAll
    for ((k, v) <- configMap) {
      println("[" + k + " = " + v + "]")
    }

    configLoader.setConfig2SparkAddFile(spark)

    var logPathSection = "local_log_path"
    val isFromMinio = configLoader.getString("general", "from_minio").toBoolean
    println("isFromMinio : " + isFromMinio)

    if (isFromMinio) {
      logPathSection = "minio_log_path"

      val endpoint = configLoader.getString("minio", "endpoint")
      val accessKey = configLoader.getString("minio", "accessKey")
      val secretKey = configLoader.getString("minio", "secretKey")
      val bucket = configLoader.getString("minio", "bucket")

      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    }
    import spark.implicits._
    val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt

    val datasetColumnStr = configLoader.getString("dataset", "setting_col")

    val datasetColumnNames = configLoader.getString("dataset", "bigtable_datatype_col")

    val datatypeTable = configLoader.getString("dataset", "bigtable_datatype_table")

    val itemInfo = configLoader.getString("dataset", "item_info_str")

    val productItemSpecTable = configLoader.getString("dataset", "product_item_spec_table")

    ///////////
    //載入資料//
    ///////////

    try {

      //先讀dataset setting table
      val mariadbUtils = new MariadbUtils()

      val datasetSql = "select setting_new.*, part.component from (select setting.id, setting.name, setting.product, " +
          "setting.bt_name, setting.bt_create_time, setting.bt_last_time, setting.bt_next_time, " +
          "setting.effective_start_date, setting.effective_end_date, item.item, item.station " +
          "from data_set_setting setting, data_set_station_item item " +
          "where setting.id=item.dss_id and setting.effective_start_date<='" + executeTime +
          "' and setting.effective_end_date>='" + executeTime + "') setting_new " +
          "left join data_set_part part on setting_new.id=part.dss_id"
      println("datasetSql:" + datasetSql)

      val datasetDf = mariadbUtils.getDfFromMariadbWithQuery(spark, datasetSql, numExecutors)
        .filter($"item".isNotNull.and($"station".isNotNull)).orderBy(col("id").asc)
      datasetDf.show(false)

//      var datasetDf = mariadbUtils.execSqlToMariadbToDf(spark, datasetSql, datasetColumnStr)
//      datasetDf.show(false)

      val datasetGroupByProductIdDF = datasetDf.groupBy("product", "id", "name")
        .agg(collect_set("station").as("station"),
          collect_set("item").as("item"),
          collect_set("component").as("component"))

      //      val datasetGroupByProductIdList = datasetGroupByProductIdDF.select("product", "id", "station", "item", "component").collect.toList
      val datasetGroupByProductIdList = datasetGroupByProductIdDF.select("product", "id", "station", "component").collect.toList


      //依每個資料集id建大表
      for (row <- datasetGroupByProductIdList) {
        val id = row.getAs[Long]("id").toString
        println("gen bigtable id: " + id + " start_time:" + new SimpleDateFormat(
          configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))
        val currentDatasetDf = datasetGroupByProductIdDF.where("id='" + id + "'")
        val product = currentDatasetDf.select("product").map(_.getString(0)).collect().mkString("")
        val stationList = currentDatasetDf.selectExpr("explode(station)").dropDuplicates().map(_.getString(0)).collect.toList
        val itemList = currentDatasetDf.selectExpr("explode(item)").dropDuplicates().map(_.getString(0)).collect.toList

        //找出工站與測項的值
        val currentDatasetStationItemDf = datasetDf.select("station", "item").where("id='" + id + "'")
          .groupBy("station").agg(collect_set("item").as("item"))
          .withColumnRenamed("station", "station_name")

        //create bigtable
        val testDeailResultGroupByFirstDf = Bigtable.createBigtable(spark, row, currentDatasetDf.drop("item"),
          currentDatasetStationItemDf, id, jobStartTime, nextExcuteTime)

        //展開json欄位匯出csv提供客戶下載, 並將大表欄位儲存起來
        val (jsonColumnMapping, columnNames) = Export.exportBigtableToCsv(spark, currentDatasetDf, currentDatasetStationItemDf, id, testDeailResultGroupByFirstDf)
println("-----------------> extract bigtable column datatype: " + id + ", start_time:" + new SimpleDateFormat(
  configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

println("id :" + id + " sn count:" + testDeailResultGroupByFirstDf.select("sn").dropDuplicates().count())
        //release memory
        testDeailResultGroupByFirstDf.unpersist()

        //存大表的欄位型態datatype到mysql
        //讀取datatype欄位
        val dataTypeCondition = "product = '" + product + "'" + " and station_name in (" + stationList.map(s => "'" + s + "'").mkString(",") + ")" +
          " and test_item in (" + itemList.map(s => "'" + s + "'").mkString(",") + ")"
        val dataTypeSql = "select test_item,test_item_datatype from " + productItemSpecTable + " where " + dataTypeCondition
        val dataTypeDF = mariadbUtils.getDfFromMariadbWithQuery(spark, dataTypeSql, numExecutors)
        val dataTypeMap = dataTypeDF.select($"test_item", $"test_item_datatype").as[(String, String)].collect.toMap

        var datasetColumnsList = List[Row]()
        for (column <- columnNames) {
          var jsonType = "fixed"
          if (jsonColumnMapping.contains(column)) {
            jsonType = jsonColumnMapping.apply(column)
          }
          var columnTemp = column
          var dataType = "string"
          if (jsonType.equals(itemInfo) && column.contains("@")) {
            columnTemp = column.split("@")(1)
          }
          if (dataTypeMap.contains(columnTemp)) {
            dataType = dataTypeMap.apply(columnTemp)
          }
          datasetColumnsList = datasetColumnsList :+ Row(id, column, dataType, jsonType)
        }
        val rdd = spark.sparkContext.makeRDD(datasetColumnsList)

        //記錄dataset大表欄位
        val datasetSchema = IoUtils.createStringSchema(datasetColumnNames)

        val datasetDataTypeDf = spark.createDataFrame(rdd, datasetSchema)

        //delete bigdata datatype in dataset id
        val deleteSql = "DELETE FROM " + datatypeTable + " WHERE data_set_id='" + id + "'"
        mariadbUtils.execSqlToMariadb(deleteSql)
        mariadbUtils.saveToMariadb(datasetDataTypeDf, datatypeTable, numExecutors)
println("-----------------> extract bigtable column datatype: " + id + ", end_time:" + new SimpleDateFormat(
  configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

println("gen bigtable id: " + id + " end_time:" + new SimpleDateFormat(
  configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

      }

      val jobEndTime: String = new SimpleDateFormat(
        configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime())
      println("job end time : " + jobEndTime)

    } catch {
      case ex: FileNotFoundException => {
        // ex.printStackTrace()
        println("===> FileNotFoundException !!!")
      }
    }
  }

}
