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
import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.expressions.Window


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
        configLoader.getString("summary_log_path","job_fmt")).format(date.getTime())
    println("job start time : " + jobStartTime)
    val executeTime: String = new SimpleDateFormat(
      configLoader.getString("log_prop","product_dt_fmt")).format(date.getTime())
    println("execute time : " + executeTime)
    val nextExcuteDate = org.apache.commons.lang.time.DateUtils.addMinutes(date, 60)
    val nextExcuteTime = new SimpleDateFormat(
      configLoader.getString("summary_log_path","job_fmt")).format(nextExcuteDate.getTime())
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

    //val factory = configLoader.getString("general", "factory")

    //val failCondition: Int = configLoader.getString("analysis", "fail_condition").toInt

    //s3a://" + bucket + "/
    val testDetailPath = configLoader.getString(logPathSection, "test_detail_path")

    val testDetailFileLmits = configLoader.getString(logPathSection, "test_detail_file_limits").toInt

    val testDetailColumns = configLoader.getString("log_prop", "test_detail_col")

    val dataSeperator = configLoader.getString("log_prop", "log_seperator")

    ///////////
    //載入資料//
    ///////////

    try {

      //先讀dataset setting table
      val mariadbUtils = new MariadbUtils()

      val datasetColumnStr = "id,name,product,bt_name,bt_create_time,bt_last_time,bt_next_time,effective_start_date,effective_end_date,component,item,station"
      val datasetTableStr = "data_set_setting"

      val datasetSql = "select setting.id, setting.name, setting.product, setting.bt_name, setting.bt_create_time, " +
        "setting.bt_last_time, setting.bt_next_time, setting.effective_start_date, " +
        "setting.effective_end_date, part.component, item.item, item.station " +
        "from data_set_setting setting, data_set_station_item item, data_set_part part " +
        "where setting.id=item.dss_id and setting.id=part.dss_id " +
        "and setting.effective_start_date<='" + executeTime + "' "+
        "and setting.effective_end_date>='" + executeTime + "' "

      val datasetDf = mariadbUtils.execSqlToMariadbToDf(spark, datasetSql, datasetColumnStr)
        .filter($"item".isNotNull.and($"station".isNotNull).and($"component".isNotNull))

      val datasetGroupByProductIdDF = datasetDf.groupBy("product", "id", "name")
        .agg(collect_set("station").as("station"),
        collect_set("item").as("item"),
          collect_set("component").as("component"))
datasetGroupByProductIdDF.select("product", "id", "station", "item").show(false)

      val productList = datasetGroupByProductIdDF.select("product").dropDuplicates().map(_.getString(0)).collect.toList
      val stationList = datasetGroupByProductIdDF.selectExpr("explode(station)").dropDuplicates().map(_.getString(0)).collect.toList


      var testdetailDF = datasetDf.groupBy("product")
        .agg(collect_set("station").as("station"),
          collect_set("item").as("item"))

      val count = datasetGroupByProductIdDF.count()
      println("count: " + count + " row")
      if(count == 0){
        sys.exit
      }

      //sample sql
      //      val sql = "select * from public.test_detail where station_name='TLEOL'" +
      //        "and product = 'TaiJi Base' and (array_length(array_positions(test_item, 'ProcPCClockSync^DResultInfo'), 1)>0" +
      //        "or array_length(array_positions(test_item, 'ProcPCClockSync^DResultInfo2'), 1)>0)"
      //      IoUtils.getDfFromCockroachdb(spark, sql)

      //test value
      testdetailDF = testdetailDF.withColumn("selectSql",
        genTestDetailItemSelectSQL(lit("test_value"), col("item")))
//testdetailDF.select("selectSql").show(false)

      //testdetail filter sql
      var testdetailFilterColumnStr = configLoader.getString("log_prop", "test_detail_filter_col")

      testdetailFilterColumnStr = testdetailFilterColumnStr + testdetailDF.select("selectSql").first().mkString("").toString()
      val selectSql = "select " + testdetailFilterColumnStr + " from test_detail"
println(selectSql)
      testdetailDF = testdetailDF.withColumn("whereSql",
        genTestDetailWhereSQL(col("product"), col("station"), col("item")))

      //撈測試結果細表的條件
      val selectSqlList = testdetailDF.select("whereSql").map(_.getString(0)).collect.toList
      val testDeailResultDf = IoUtils.getDfFromCockroachdb(spark, selectSqlList,
        testdetailFilterColumnStr, "whereSql", selectSql)
//testDeailResultDf.show(false)
      //group by sn, staion_name, order by test_starttime
      val wSpecTestDetailAsc = Window.partitionBy(col("sn"), col("station_name"))
        .orderBy(asc("test_starttime"))

      val wSpecTestDetailDesc = Window.partitionBy(col("sn"), col("station_name"))
        .orderBy(desc("test_starttime"))

      var testDeailResultGroupByFirstDf = testDeailResultDf
        .withColumn("asc_rank", rank().over(wSpecTestDetailAsc))
        .withColumn("desc_rank", rank().over(wSpecTestDetailDesc))
        .where($"asc_rank".equalTo(1).or($"desc_rank".equalTo(1)))
        .withColumn("value_rank", getFirstOrLastRow($"asc_rank", $"desc_rank"))
        .withColumn("value_rank", explode($"value_rank"))

      //以每個dataset, 收斂成一個工站資訊
      val station_name_str ="station_name"
      val item_str ="item"
      val station_info_str ="station_info"
      val item_info_str ="item_info"

      testDeailResultGroupByFirstDf = datasetGroupByProductIdDF.withColumnRenamed("product", "product_dataset")
        .join(testDeailResultGroupByFirstDf, col("product_dataset").equalTo(col("product"))
        .and(array_contains($"station", $"station_name")), "left")

      val stationInfoColumn = configLoader.getString("dataset", "station_info_col")
      var map = Map[String, String]()
      var list = List[String]()
      stationInfoColumn.split(",").foreach(attr => {
        val attrStr = attr + "_str"
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumn(attrStr, genStaionJsonFormat(col(station_name_str), lit(attr), col(attr)))
        map = map + (attrStr -> "collect_set")
        list = list :+ "collect_set(" + attrStr + ")"
      })

      var itemList = List[String]()
      //以每個dataset, 收斂成一個測項資訊
      val itemInfoColumn = configLoader.getString("dataset", "item_info_col")
      itemInfoColumn.split(",").foreach(attr => {
        val attrStr = attr + "_str"
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(attrStr,
          genItemJsonFormat(col(station_name_str), col(item_str), col(attr), lit(attr)))
        map = map + (attrStr -> "collect_set")
        itemList = itemList :+ "collect_set(" + attrStr + ")"
      })

//      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(item_info_str,
//        genItemInfo($"station_name", $"item", $"test_value"))
//        .withColumn(item_info_str,
//          genItemInfo($"station_name", $"item", $"test_value"))
//        .withColumn(item_info_str,
//          genItemInfo($"station_name", $"item", $"test_value"))
//      map = map + (item_info_str-> "collect_set")
//testDeailResultGroupByFirstDf.show(false)
      //group by 並收斂工站與測項資訊
      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.groupBy("id", "sn", "value_rank")
          .agg(map)
          .withColumn(station_info_str, array())
//        add
          .withColumn(item_info_str, array())

      list.foreach(ele => {
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumn(station_info_str, genInfo(col(station_info_str), col(ele)))
          .drop(ele) //刪除collect_set的多個工站資訊欄位
      })

      itemList.foreach(ele => {
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumn(item_info_str,
            genInfo(col(item_info_str), col(ele)))
          .drop(ele) //刪除collect_set的多個工站資訊欄位
      })

      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
       .withColumn(station_info_str, transferArrayToString(col(station_info_str)))
        .withColumn(item_info_str, transferArrayToString(col(item_info_str)))
//       .withColumn(item_info_str, transferArrayToString(col("collect_set(" + item_info_str + ")")))
//       .drop(col("collect_set(" + item_info_str + ")"))
       .join(datasetGroupByProductIdDF.select("id", "name", "product"), Seq("id"), "left")
//      新增sn_type, 紀錄是整機還是非整機測試(比對測試明細sn vs unit_number), 目前先維持定值, 之後有需要分整機或非整機測試再處理
       .withColumn("sn_type", lit("wip"))//unit
//testDeailResultGroupByFirstDf.show(false)

      //組裝主表撈出sn對應的wo
      val snList = testDeailResultGroupByFirstDf.select("sn").dropDuplicates().map(_.getString(0)).collect.toList
      //TODO for ippd test_detail sn要取前十碼才對的上part_master sn
//      val snCondition = "sn in (" + snList.map(s => "'" + s.substring(0, 10) + "'").mkString(",") + ")"
        val snCondition = "sn in (" + snList.map(s => "'" + s + "'").mkString(",") + ")"
//      val partMasterPredicates = Array[String](snCondition)
      println(snCondition)
      val masterTable = configLoader.getString("log_prop", "wip_table")
//      var partMasterDf = IoUtils.getDfFromCockroachdb(spark, masterTable , partMasterPredicates)

      var masterWhereSql = "select id,floor,wo,scantime,sn from " + masterTable + " where " + snCondition
      var partMasterDf = IoUtils.getDfFromCockroachdb(spark, masterWhereSql)

      //group by sn, order by scantime asc, 取第一筆
      val wSpecPartMasterAsc = Window.partitionBy(col("sn"))
        .orderBy(asc("scantime"))

      partMasterDf = partMasterDf
        .withColumn("rank", rank().over(wSpecPartMasterAsc))
        .where($"rank".equalTo(1))

      val partMasterIdList = partMasterDf.select("id").dropDuplicates().map(_.getString(0)).collect.toList

      val woList = partMasterDf.select("wo").dropDuplicates().map(_.getString(0)).collect.toList
      //TODO for ippd part_master wo要取後三碼才對的上worker_order wo
//      val woCondition = "wo in (" + woList.map(s => "'" + s.substring(3, s.length) + "'").mkString(",") + ")"
      val woCondition = "wo in (" + woList.map(s => "'" + s + "'").mkString(",") + ")"
//      val woPredicates = Array[String](woCondition)
      println(woCondition)
      val woTable = configLoader.getString("log_prop", "wo_table")
//      var woDf = IoUtils.getDfFromCockroachdb(spark, configLoader.getString("log_prop", "wo_table"), woPredicates)

      var woWhereSql = "select * from " + woTable + " where " + woCondition
      var woDf = IoUtils.getDfFromCockroachdb(spark, woWhereSql)

      //只撈關鍵物料的component
      val componentList = datasetDf.select("component").dropDuplicates().map(_.getString(0)).collect.toList
      val configList = woDf.select("config").dropDuplicates().map(_.getString(0)).collect.toList

      val componentCondition = "config in (" + configList.map(s => "'" + s + "'").mkString(",") + ") " +
        "and component in (" +   componentList.map(s => "'" + s + "'").mkString(",") + ")"
      println(componentCondition)
      val comTable = configLoader.getString("log_prop", "mat_table")
//      val componentPredicates = Array[String](componentCondition)
//      var comConfigDf = IoUtils.getDfFromCockroachdb(spark, matTable, componentPredicates)
//        .select("config","vendor", "hhpn", "oempn", "component", "component_type", "input_qty")
      var comWhereSql = "select config,vendor,hhpn,oempn,component,component_type,input_qty from " + comTable + " where " + componentCondition
      var comDf = IoUtils.getDfFromCockroachdb(spark, comWhereSql)

//      先用dataset選的關鍵物料
//      val componentList = comDf.select("component").dropDuplicates().map(_.getString(0)).collect.toList
      val partDetailCondition = "id in (" +partMasterIdList.map(s => "'" + s + "'").mkString(",") + ") " +
        " and part in (" + componentList.map(s => "'" + s + "'").mkString(",") + ")"
//      val partDetailCondition = "partsn in (" + configList.map(s => "'" + s + "'").mkString(",") + ")"
//      val partsnPredicates = Array[String](partDetailCondition)
//      var partDetailDf = IoUtils.getDfFromCockroachdb(spark, detailTable, partsnPredicates)
//          .dropDuplicates("partsn", "cust_part", "vendor_code", "date_code")
      println(partDetailCondition)
      val detailTable = configLoader.getString("log_prop", "wip_parts_table")
      var partDetailWhereSql = "select id,partsn,scantime,vendor_code,date_code,part from " + detailTable + " where " + partDetailCondition
      var partDetailDf = IoUtils.getDfFromCockroachdb(spark, partDetailWhereSql)

      val partList = partDetailDf.select("part").dropDuplicates().map(_.getString(0)).collect.toList
      val partCondition = "part in (" + partList.map(s => "'" + s + "'").mkString(",") + ")"
      println(partCondition)

      //group by part, order by scantime asc, 取第一筆
      val wSpecDetailAsc = Window.partitionBy(col("part"))
        .orderBy(asc("scantime"))

      val partDetailColumnStr = configLoader.getString("dataset", "part_detail_col")
      val partDetailColumn = partDetailColumnStr.split(",")
      partDetailDf = partDetailDf
        .withColumn("rank", rank().over(wSpecDetailAsc))
        .where($"rank".equalTo(1))
        //.select("partsn", "vendor_code", "date_code", "part")
        .selectExpr(partDetailColumn: _*)
        .withColumnRenamed("part", "component")


      comDf = comDf.join(partDetailDf, Seq("component"),"left")

      //以每個dataset, 收斂成一個關鍵物料資訊
      val componentInfoStr = "vendor,hhpn,oempn,component_type,input_qty"
      val component_str ="component"
      val component_info_str ="component_info"
      val componentInfoColumn = configLoader.getString("dataset", "component_info_col")

      var comConfigMap = Map[String, String]()
      var comConfigList = List[String]()
      componentInfoColumn.split(",").foreach(attr => {
        var attrStr = attr + "_str"
        comDf = comDf
          .withColumn(attrStr, genStaionJsonFormat(col(component_str), lit(attr), col(attr)))
        comConfigMap = comConfigMap + (attrStr -> "collect_set")
        comConfigList = comConfigList :+ "collect_set(" + attrStr + ")"
      })

      var datasetComponentDF = testDeailResultGroupByFirstDf.select("id", "sn").dropDuplicates()
          .join(datasetGroupByProductIdDF.select("id", "component").dropDuplicates(), Seq("id"))
          .withColumn("component", explode(col("component")))
          .join(comDf, Seq("component"), "left")

      //group by 並收斂關鍵物料資訊
      datasetComponentDF = datasetComponentDF.groupBy("id", "sn", "config")
        .agg(comConfigMap)
        .withColumn(component_info_str, array())

      comConfigList.foreach(ele => {
        datasetComponentDF = datasetComponentDF
          .withColumn(component_info_str, genInfo(col(component_info_str), col(ele)))
          .drop(ele) //刪除collect_set的多個工站資訊欄位
      })

      partMasterDf = partMasterDf
        .select("sn", "floor", "scantime", "wo")
        .dropDuplicates()
        //TODO for ippd part_master wo要取後三碼開始才對的上worker_order wo
//        .withColumn("wo", expr("substring(wo, 4, length(wo))"))
      //一個工單號或對到多個config嗎
      woDf = woDf.select("wo", "wo_type", "plant_code", "plan_qty", "config", "build_name", "release_date")
      //group by wo, order by release_date desc, 取第一筆
      val wSpecWoDesc = Window.partitionBy(col("wo"))
        .orderBy(desc("release_date"))
      woDf = woDf.withColumn("rank", rank().over(wSpecWoDesc))
        .where($"rank".equalTo(1))
        .drop("rank")
        .drop("release_date")
        .dropDuplicates("wo")
      woDf = partMasterDf.select("wo", "floor", "scantime").join(woDf, Seq("wo"), "left")
      woDf = woDf.join(datasetComponentDF, Seq("config"), "left")

//partMasterDf.show(false)
//woDf.show(false)

      //join 工單與關鍵物料
      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
        .join(woDf, Seq("id", "sn"), "left")
        .withColumn(component_info_str, transferArrayToString(col(component_info_str)))

  testDeailResultGroupByFirstDf.show(25, false)

      //create dataset bigtable schema
      var schema = "`data_set_name` varchar(200) Not NULL,"+
        "`data_set_id` varchar(30) Not NULL," +
        "`product` varchar(50) Not NULL," +
        "`sn` varchar(100) Not NULL,"+
        "`sn_type` varchar(100) DEFAULT NULL,"+
        "`value_rank` varchar(30) Not NULL,"+
        "`station_info` json Not NULL,"+
        "`item_info` json Not NULL,"+
        "`floor` varchar(50) DEFAULT NULL,"+ //組裝樓層
        "`scantime` datetime DEFAULT NULL,"+
        "`wo` varchar(50) DEFAULT NULL,"+
        "`wo_type` varchar(50) DEFAULT NULL,"+
        "`plant_code` varchar(50) DEFAULT NULL,"+
        "`plan_qty` varchar(50) DEFAULT NULL,"+
        "`config` varchar(50) DEFAULT NULL,"+
        "`build_name` varchar(50) DEFAULT NULL,"+
        "`component_info` json DEFAULT NULL,"+
        "PRIMARY KEY (`data_set_id`,`product`,`sn`,`value_rank`)"+
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
        .withColumnRenamed("name","data_set_name")
        .withColumnRenamed("id","data_set_id")

      val datasetIdDF = testDeailResultGroupByFirstDf.select("data_set_id").dropDuplicates("data_set_id")
      val idList = datasetIdDF.map(_.getString(0)).collect.toList
      for (id <- idList) {
        val datasetTableName = "`data_set_bigtable@"+id+"`"
        //drop dataset bigtable
        val dropSql = "DROP TABLE IF EXISTS " + datasetTableName
        mariadbUtils.execSqlToMariadb(dropSql)
        //truncate dataset bigtable schema
        val createSql = "CREATE TABLE " + datasetTableName + " ("+ schema
        mariadbUtils.execSqlToMariadb(createSql)

//      insert 大表資料
        val datasetOutputDF = testDeailResultGroupByFirstDf.filter(col("data_set_id").equalTo(id))
        mariadbUtils.saveToMariadb(datasetOutputDF, datasetTableName, numExecutors)
//      update dataset 設定的欄位
        val updateSql = "UPDATE data_set_setting"+" SET bt_name='"+datasetTableName.substring(1,datasetTableName.length-1)+"'," +
        " bt_create_time = COALESCE(bt_create_time, '"+jobStartTime+"')," +
        " bt_last_time = '" + jobStartTime + "'," +
        " bt_next_time = '" + nextExcuteTime + "'" +
        " WHERE id = " + id
        mariadbUtils.execSqlToMariadb(updateSql)
      }

      //展開json欄位匯出csv提供客戶下載, 並將大表欄位儲存起來
      val jsonColumns = List(station_info_str, item_info_str, component_info_str)

      var datasetOutputDF = testDeailResultGroupByFirstDf
      for(jsonInfo <- jsonColumns){
        datasetOutputDF = datasetOutputDF.withColumn(jsonInfo, regexp_replace(col(jsonInfo), "'", ""))
      }

      //記錄dataset大表欄位
      val datasetColumnNames = "data_set_id,column_name,column_type,json_str"
      val datasetSchema = IoUtils.createStringSchema(datasetColumnNames)

      //station 與 component json固定資訊
      var jsonMap = Map(station_info_str -> stationInfoColumn, component_info_str -> componentInfoColumn)

      val dataTypePredicates = Array[String]("product in (" + productList.map(s => "'" + s + "'").mkString(",") +")"
        + " and station_name in (" + stationList.map(s => "'" + s + "'").mkString(",")+")")
      val dataTypeDF = mariadbUtils.getDfFromMariadb(spark, "product_item_spec", dataTypePredicates).select("test_item", "test_item_datatype")


      val dataTypeMap = dataTypeDF.select($"test_item", $"test_item_datatype").as[(String, String)].collect.toMap

      for (id <- idList) {
        var datasetOutputCsvDF = datasetOutputDF.filter($"id".equalTo(id))
        //取第一筆, 先展開json object, 存成csv
        var datasetById = datasetGroupByProductIdDF.filter($"id".equalTo(id))

        var jsonColumnMapping = Map[String, String]()
        for(jsonInfo <- jsonColumns){
          var datasetColumnName = jsonInfo.replace("_info", "")
          datasetById = datasetById.withColumn(datasetColumnName, explode(col(datasetColumnName)))
          val infoList = datasetById.select(datasetColumnName).dropDuplicates().map(_.getString(0)).collect.toList
          var infoFielids = List[String]()
          if(jsonInfo.equals(item_info_str)){
            //jsonMap += (item_info_str -> infoList.mkString(","))
            val stationInfo = datasetById.select("station").dropDuplicates().map(_.getString(0)).collect.mkString(",")
            stationInfo.split(",").map(
              station => {
                for(info <- infoList){
                  val jsonValue = station + "@" + info
                  infoFielids = infoFielids :+ jsonValue
                  val jsonResult = jsonValue + "@result"
                  infoFielids = infoFielids :+ jsonResult
                  val jsonResultDetail = jsonValue + "@result_detail"
                  infoFielids = infoFielids :+ jsonResultDetail
                  jsonColumnMapping = jsonColumnMapping + (jsonValue -> jsonInfo)
                  jsonColumnMapping = jsonColumnMapping + (jsonResult -> jsonInfo)
                  jsonColumnMapping = jsonColumnMapping + (jsonResultDetail -> jsonInfo)
                }
              }
            )
          }

          if(jsonInfo.equals(station_info_str) || jsonInfo.equals(component_info_str)){
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

          datasetOutputCsvDF = datasetOutputCsvDF.withColumn(jsonInfo, from_json(col(jsonInfo), schema))
//          datasetOutputCsvDF.printSchema()
          for (column <- infoFielids) {
//            println(jsonInfo+"."+column)
//            datasetOutputCsvDF = datasetOutputCsvDF.withColumn(column, col(jsonInfo+"."+column))
            datasetOutputCsvDF = datasetOutputCsvDF.withColumn(column, col(jsonInfo).getItem(column))
          }
        }

        for(jsonInfo <- jsonColumns){
          datasetOutputCsvDF = datasetOutputCsvDF.drop(jsonInfo)
        }
//datasetOutputCsvDF.show(false)
        datasetOutputCsvDF.coalesce(1).write.option("header", "true")
          .mode("overwrite").csv(configLoader.getString("minio_log_path", "dataset_path")+"/"+id)
        //存大表的欄位型態datatype到mysql
        val columnNames = datasetOutputCsvDF.columns
        var datasetColumnsList = List[Row]()
        for(column <- columnNames){
          var jsonType = "fixed"
          if(jsonColumnMapping.contains(column)){
            jsonType = jsonColumnMapping.apply(column)
          }
          var columnTemp = column
          var dataType = "string"
          if(jsonType.equals(item_info_str) && column.contains("@")){
            columnTemp = column.split("@")(1)
          }
          if(dataTypeMap.contains(columnTemp)){
            dataType = dataTypeMap.apply(columnTemp)
          }
          datasetColumnsList = datasetColumnsList :+ Row(id, column, dataType, jsonType)
        }
        val rdd = spark.sparkContext.makeRDD(datasetColumnsList)
        val datasetDataTypeDf = spark.createDataFrame(rdd, datasetSchema)
        //delete bigdata datatype in dataset id
        val datatypeTable = configLoader.getString("dataset", "bigtable_datatype_table")
        val deleteSql = "DELETE FROM " + datatypeTable + " WHERE data_set_id='" + id + "'"
        mariadbUtils.execSqlToMariadb(deleteSql)
        mariadbUtils.saveToMariadb(datasetDataTypeDf, datatypeTable, numExecutors)

      }

      val jobEndTime: String = new SimpleDateFormat(
        configLoader.getString("summary_log_path","job_fmt")).format(new Date().getTime())
      println("job end time : " + jobEndTime)

    } catch {
      case ex: FileNotFoundException => {
        // ex.printStackTrace()
        println("===> FileNotFoundException !!!")
      }
    }
  }

}
