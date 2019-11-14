package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.net.InetAddress
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale, Properties}
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util
import java.util.concurrent.TimeUnit

import com.foxconn.iisd.bd.rca.SummaryFile._

//import com.foxconn.iisd.bd.rca.Bigtable.mariadbUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{regexp_extract, regexp_replace, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.foxconn.iisd.bd.rca.SparkUDF._
import com.foxconn.iisd.bd.rca.ConfigLoader
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel


object XWJBigtable {
  // create configLoader object
  var configLoader = new ConfigLoader()
  // create job object
  val job = new Job()
  // create configContext object
  val configContext = new ConfigContext()

  def main(args: Array[String]): Unit = {

    println("xwj-bigtable-v4:")

    //關閉不必要的Log資訊
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Job開始
    println("======> XWJ Bigtable Job Start")
    job.jobStartTime = new java.util.Date()

    ////////////////////////////////////////////////////////////////////////////////////////
    //初始化作業
    ////////////////////////////////////////////////////////////////////////////////////////
    initialize(args)
    job.setJobId(configContext)
    val jobStartTime: String = new SimpleDateFormat(configContext.jobDateFmt).format(job.jobStartTime.getTime())
    println("job start time : " + jobStartTime)

    ////////////////////////////////////////////////////////////////////////////////////////
    //XWJ Bigtable處理
    ////////////////////////////////////////////////////////////////////////////////////////
    val sparkSession = configContext.sparkSession
    import sparkSession.implicits._

    val executeTime: String = new SimpleDateFormat(configContext.productDtFmt).format(job.jobStartTime.getTime())
    println("execute time : " + executeTime)
    val nextExcuteDate = org.apache.commons.lang.time.DateUtils.addMinutes(job.jobStartTime, 60)
    val nextExcuteTime = new SimpleDateFormat(configContext.jobDateFmt).format(nextExcuteDate.getTime())
    println("next execute time : " + nextExcuteTime)

    //先讀dataset setting table
    val datasetSql = "select setting_new.*, part.component from (select setting.id, setting.name, setting.product, " +
      "setting.bt_name, setting.bt_create_time, setting.bt_last_time, setting.bt_next_time, " +
      "setting.effective_start_date, setting.effective_end_date, item.item, item.station " +
      "from data_set_setting setting, data_set_station_item item " +
      "where setting.id=item.dss_id and setting.effective_start_date<='" + executeTime +
      "' and setting.effective_end_date>='" + executeTime + "') setting_new " +
      "left join data_set_part part on setting_new.id=part.dss_id"
    println("datasetSql: " + datasetSql)

    val datasetDf = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession,
      datasetSql, configContext.sparkNumExcutors).filter($"item".isNotNull.and($"station".isNotNull)).orderBy(col("id").asc)
    datasetDf.show(false)

    val datasetGroupByProductIdDF = datasetDf.groupBy("product", "id", "name")
      .agg(collect_set("station").as("station"),
        collect_set("item").as("item"),
        collect_set("component").as("component"))

    val datasetGroupByProductIdList = datasetGroupByProductIdDF.select("product", "id", "station", "component").collect.toList

    //紀錄每個dataset執行的時間
    var datasetList = List[Dataset]()

    //依每個資料集id建大表
    for (row <- datasetGroupByProductIdList) {
      val id = row.getAs[Long]("id").toString

      val startTime = new Date().getTime()
      println("gen bigtable id: " + id + ", start_time:" + new SimpleDateFormat(
        configContext.jobDateFmt).format(startTime))
      val currentDatasetDf = datasetGroupByProductIdDF.where("id='" + id + "'")
      val product = currentDatasetDf.select("product").map(_.getString(0)).collect().mkString("")
      val stationList = currentDatasetDf.selectExpr("explode(station)").dropDuplicates().map(_.getString(0)).collect.toList
      val itemList = currentDatasetDf.selectExpr("explode(item)").dropDuplicates().map(_.getString(0)).collect.toList

      //找出工站與測項的值
      val currentDatasetStationItemDf = datasetDf.select("station", "item").where("id='" + id + "'")
        .groupBy("station").agg(collect_set("item").as("item"))
        .withColumnRenamed("station", "station_name")

      //create bigtable
      val (testDeailResultGroupByFirstDf, fieldsColumnDataType) = Bigtable.createBigtable(configContext, row, currentDatasetDf.drop("item"),
        currentDatasetStationItemDf, id, jobStartTime, nextExcuteTime)

      //展開json欄位匯出csv提供客戶下載, 並將大表欄位儲存起來
      val (jsonColumnMapping, columnNames) = Export.exportBigtableToCsv(configContext, currentDatasetDf,
        currentDatasetStationItemDf, id, testDeailResultGroupByFirstDf)

      println("-----------------> extract bigtable column datatype: " + id + ", start_time:" + new SimpleDateFormat(
        configContext.jobDateFmt).format(new Date().getTime()))
      val snCount = testDeailResultGroupByFirstDf.select("sn").dropDuplicates().count()
      println("id :" + id + ", sn count:" + snCount)

      //紀錄datatype
      val fieldDataTypeMap = fieldsColumnDataType.map(field => (field.name, field.dataType)).toMap
      //release memory
      testDeailResultGroupByFirstDf.unpersist()

      //存大表的欄位型態datatype到mysql
      //讀取datatype欄位
      val dataTypeCondition = "product = '" + product + "'" + " and station_name in (" + stationList.map(s => "'" + s + "'").mkString(",") + ")" +
        " and test_item in (" + itemList.map(s => "'" + s + "'").mkString(",") + ")"
      val dataTypeSql = "select test_item,test_item_datatype from " + configContext.mysqlProductItemSpecTable + " where " + dataTypeCondition
      val dataTypeDF = configContext.mysqlDBIo.getDfFromMariadbWithQuery(configContext.sparkSession,
        dataTypeSql, configContext.sparkNumExcutors)

      val dataTypeMap = dataTypeDF.select($"test_item", $"test_item_datatype").as[(String, String)].collect.toMap

      var datasetColumnsList = List[Row]()
      for (column <- columnNames) {
        var jsonType = "fixed"
        if (jsonColumnMapping.contains(column)) {
          jsonType = jsonColumnMapping.apply(column)
        }

        //判斷datatype
        var dataType = "string"
        var columnTemp = column
        if (column.contains("@")) {
          //split 只切割第一個@ =>
          // 1. TLEOL@PcaVerifyFirmwareRev^DActualFWUpdate => PcaVerifyFirmwareRev^DActualFWUpdate
          // 2. TLEOL@PcaVerifyFirmwareRev^DActualFWUpdate@result => PcaVerifyFirmwareRev^DActualFWUpdate@result
          columnTemp = column.split("@", 2)(1)
        }
        if (jsonType.equals(configContext.itemInfo) && dataTypeMap.contains(columnTemp)) {//selected item datatype
          dataType = dataTypeMap.apply(columnTemp)
        }
        else if(fieldDataTypeMap.contains(columnTemp)){//select test_detail column
          dataType = fieldDataTypeMap.apply(columnTemp).toString.toLowerCase.replace("type", "").replace("eger", "")
        }

        datasetColumnsList = datasetColumnsList :+ Row(id, column, dataType, jsonType)
      }
      val rdd = configContext.sparkSession.sparkContext.makeRDD(datasetColumnsList)

      //記錄dataset大表欄位
      val datasetSchema = Utils.createStringSchema(configContext.datasetDatatypeColumns)

      val datasetDataTypeDf = configContext.sparkSession.createDataFrame(rdd, datasetSchema)

      //delete bigdata datatype in dataset id
      val deleteSql = "DELETE FROM " + configContext.mysqlProductBigtableDatatypeTable + " WHERE data_set_id='" + id + "'"
      configContext.mysqlDBIo.execSqlToMariadb(deleteSql)
      configContext.mysqlDBIo.saveToMariadb(datasetDataTypeDf, configContext.mysqlProductBigtableDatatypeTable, SaveMode.Append,
        configContext.sparkNumExcutors)
      println("-----------------> extract bigtable column datatype: " + id + ", end_time:" + new SimpleDateFormat(
        configContext.jobDateFmt).format(new Date().getTime()))
      val endTime = new Date().getTime()
      println("gen bigtable id: " + id + ", end_time:" + new SimpleDateFormat(
        configContext.jobDateFmt).format(endTime))

      //fii壓測紀錄---start
      val sec = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.MILLISECONDS)
      val s = sec % 60
      val m = (sec/60) % 60
      val h = (sec/60/60) % 24
      val datasetSpendTime = "%02d:%02d:%02d".format(h, m, s)
      val productName = row.getAs[String]("product").toString
      val datasetStartTime = new SimpleDateFormat(configContext.jobDateFmt).format(startTime)
      val datasetEndTime = new SimpleDateFormat(configContext.jobDateFmt).format(endTime)

      val insertSql = "INSERT INTO data_set_bigtable_execute_time(product, data_set_id, data_set_item_columns, " +
        "sn_count, start_time, end_time, spend_time)" +
        "VALUES ('" + productName + "','" + id + "','" + itemList.size + "','" + snCount + "','" +
        new SimpleDateFormat(configContext.jobDateFmt).format(startTime) + "','" +
        new SimpleDateFormat(configContext.jobDateFmt).format(endTime) + "','" +
        datasetSpendTime + "')"
      configContext.mysqlDBIo.execSqlToMariadb(insertSql)
      //fii壓測紀錄---end
      datasetList = datasetList :+ (new Dataset(productName, id, itemList.size, snCount, datasetStartTime, datasetEndTime, datasetSpendTime))
    }

    configContext.datasetList = datasetList

    val formatter = DateTimeFormatter.ofPattern(configContext.jobDateFmt)
    val genDatasetEndTime = LocalDateTime.now.plusHours(1).truncatedTo(ChronoUnit.HOURS).format(formatter)

    //update dataset 設定的欄位
    for (row <- datasetGroupByProductIdList) {
      val id = row.getAs[Long]("id").toString
      val datasetTableName = "`data_set_bigtable@" + id + "`"

      val updateSql = "UPDATE data_set_setting" + " SET bt_name='" + datasetTableName.substring(1, datasetTableName.length - 1) + "'," +
        " bt_create_time = COALESCE(bt_create_time, '" + jobStartTime + "')," +
        " bt_last_time = '" + jobStartTime + "'," + //排程執行時間
        " bt_next_time = '" + genDatasetEndTime + "'" + //下一次執行時間
        " WHERE id = " + id
      configContext.mysqlDBIo.execSqlToMariadb(updateSql)
    }

    //Job結束
    job.jobEndTime = new java.util.Date()
    println("======> XWJ Bigtable Job End")
    configContext.isJobState = true
    val jobEndTime: String = new SimpleDateFormat(configContext.jobDateFmt).format(job.jobEndTime.getTime())
    println("job end time : " + jobEndTime)

    ////////////////////////////////////////////////////////////////////////////////////////
    //SummaryFile輸出
    ////////////////////////////////////////////////////////////////////////////////////////
    SummaryFile.save(configContext)

  }

  /*
   *
   *
   * @author JasonLai
   * @date 2019/9/27 下午4:50
   * @param [args]
   * @return void
   * @description 初始化 configloader, spark, configContext
   */
  def initialize(args: Array[String]): Unit = {

    //load config yaml
    configLoader.setDefaultConfigPath("""conf/default.yaml""")
    if(args.length == 1) {
      configLoader.setDefaultConfigPath(args(0))
    }

    //env
    configContext.env = configLoader.getString("general", "env")

    //spark
    configContext.sparkJobName = configLoader.getString("spark", "job_name")
    configContext.sparkMaster = configLoader.getString("spark", "master")

    //create spark session object
    val sparkBuilder = SparkSession
      .builder
      .appName(configContext.sparkJobName)

    if(configContext.env.equals(XWJBigtableConstants.ENV_LOCAL)) {
      sparkBuilder.master(configContext.sparkMaster)
    }

    val spark = sparkBuilder.getOrCreate()

    //spark executor add config file
    configLoader.setConfig2SparkAddFile(spark)

    //executor instances number
    configContext.sparkNumExcutors = spark.conf.get("spark.executor.instances", "1").toInt

    //add sparkSession into configContext
    configContext.sparkSession = spark

    //初始化context
    initializeconfigContext()

    println(s"======> Job Start Time : ${configContext.job.jobStartTime}")
    println(s"======> Job Flag : ${configContext.flag}")
  }

  /*
   *
   *
   * @author EchoLee
   * @date 2019/9/19 上午9:37
   * @description configContext初始化基本設定值
   */
  def initializeconfigContext(): Unit = {
    //job
    configContext.job = job
    configContext.jobDateFmt = configLoader.getString("summary_log_path", "job_fmt")
    //flag
    configContext.flag = job.jobStartTime.getTime.toString

    //product
    configContext.productDtFmt = configLoader.getString("log_prop", "product_dt_fmt")
    configContext.datasetDatatypeColumns = configLoader.getString("dataset", "bigtable_datatype_col")
    configContext.itemInfo = configLoader.getString("dataset", "item_info_str")
    configContext.stationInfo = configLoader.getString("dataset", "station_info_str")
    configContext.componentInfo = configLoader.getString("dataset", "component_info_str")
    configContext.stationInfoColumns = configLoader.getString("dataset", "station_info_col")
    configContext.itemInfoColumns = configLoader.getString("dataset", "item_info_col")
    configContext.componentInfoColumns = configLoader.getString("dataset", "component_info_col")
    configContext.datasetPath = configLoader.getString("minio_log_path", "dataset_path")
    //bigtable
    configContext.testDetailFilterColumns = configLoader.getString("log_prop", "test_detail_filter_col")
    configContext.detailFilterColumns = configLoader.getString("log_prop", "wip_parts_filter_col")
    configContext.detailColumns = configLoader.getString("dataset", "part_detail_col")
    configContext.stationName = configLoader.getString("dataset", "station_name_str")
    configContext.item = configLoader.getString("dataset", "item_str")
    configContext.component = configLoader.getString("dataset", "component_str")
    configContext.numPartitions = configLoader.getString("dataset", "numPartitions").toInt

    //minio
    configContext.minioEndpoint = configLoader.getString("minio", "endpoint")
    configContext.minioConnectionSslEnabled = configLoader.getString("minio", "connectionSslEnabled").toBoolean
    configContext.minioAccessKey = configLoader.getString("minio", "accessKey")
    configContext.minioSecretKey = configLoader.getString("minio", "secretKey")
    //to summaryfile use
    configContext.minioBucket = configLoader.getString("minio", "bucket")
    //cockroach db
    configContext.cockroachDbConnUrlStr = configLoader.getString("cockroachdb", "conn_str")
    configContext.cockroachDbSslMode = configLoader.getString("cockroachdb", "sslmode")
    configContext.cockroachDbUserName = configLoader.getString("cockroachdb", "username")
    configContext.cockroachDbPassword = configLoader.getString("cockroachdb", "password")
    configContext.cockroachDbMasterTable = configLoader.getString("log_prop", "wip_table")
    configContext.cockroachDbDetailTable = configLoader.getString("log_prop", "wip_parts_table")
    configContext.cockroachDbTestTable = configLoader.getString("log_prop", "bobcat_table")
    configContext.cockroachDbTestDetailTable = configLoader.getString("log_prop", "test_detail_table")
    configContext.cockroachDbWoTable = configLoader.getString("log_prop", "wo_table")
    configContext.cockroachDbMatTable = configLoader.getString("log_prop", "mat_table")
    configContext.cockroachDbMasterColumns = configLoader.getString("log_prop", "wip_filter_col")
    configContext.cockroachDbDetailColumns = configLoader.getString("log_prop", "wip_parts_line_col")

    //mysql db
    configContext.mysqlDbConnUrlStr = configLoader.getString("mariadb", "conn_str")
    configContext.mysqlDatabase = configLoader.getString("mariadb", "database")
    configContext.mysqlDbUserName = configLoader.getString("mariadb", "username")
    configContext.mysqlDbPassword = configLoader.getString("mariadb", "password")
    configContext.mysqlProductItemSpecTable = configLoader.getString("mariadb", "product_item_spec_table")
    configContext.mysqlProductStationTable = configLoader.getString("mariadb", "product_station_table")
    configContext.mysqlProductMatTable = configLoader.getString("mariadb", "config_component_table")
    configContext.mysqlProductBigtableDatatypeTable = configLoader.getString("mariadb", "bigtable_datatype_table")
    //summary file
    configContext.summaryFileLogBasePath = configLoader.getString("summary_log_path", "data_base_path")
    configContext.summaryFileLogTag = configLoader.getString("summary_log_path", "tag")
    configContext.summaryFileExtension = configLoader.getString("summary_log_path", "file_extension")
    configContext.summaryFileJobFmt = configLoader.getString("summary_log_path", "job_fmt")
    configContext.summaryFileBuName = configLoader.getString("summary_log_path", "bu_name")

    //object
    val fileDataSource = new FileSource(configContext)
    fileDataSource.init()
    configContext.fileDataSource = fileDataSource

    val cockroachDBIo = new CockroachDBIo(configContext)
    configContext.cockroachDBIo = cockroachDBIo

    val mysqlDBIo = new MysqlDBIo(configContext)
    configContext.mysqlDBIo = mysqlDBIo

  }
}
