package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{regexp_extract, regexp_replace, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import com.foxconn.iisd.bd.rca.SparkUDF._
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


    var date: java.util.Date = new java.util.Date()
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

    //"sn,build_name,build_description,unit_number,station_id,test_status,test_starttime,test_endtime,list_of_failure,list_of_failure_detail,test_phase,machine_id,factory_code,floor,line_id,test_item,test_value,test_unit,test_lower,test_upper,create_time,update_time,station_name,start_date,product,test_version"
    //CN95I870ZC06MD_||_SOR_||_SOR_||_CN95I870ZC06MD_||_L7_TLEOL_06_||_Exception_||_2019/05/18 06:36_||_2019/05/18 06:36_||_PcaVerifyFirmwareRev_||_Error_||_MP_||__||_CQ_||_D62_||_2_||_ProcPCClockSync^DResultInfo^APcaVerifyFirmwareRev^DResultInfo^APcaVerifyFirmwareRev^DExpectedVersion^APcaVerifyFirmwareRev^DReadVersion^APcaVerifyFirmwareRev^DDateTimeStarted^APcaVerifyFirmwareRev^DActualFWUpdate^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C5/18/2019 5:29:48 AM^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_2019/05/18 06:36_||_2019/05/18 06:36_||_TLEOL_||_2019/05/18 06:36_||_TaiJi Base_||_42.3.8 REV_37_Taiji25
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
        .filter($"item".isNotNull.and($"station".isNotNull))

      var testdetailGroupByProductIdDF = datasetDf.groupBy("product", "id", "name")
        .agg(collect_set("station").as("station"),
        collect_set("item").as("item"),
          collect_set("component").as("component"))

      var testdetailDF = datasetDf.groupBy("product")
        .agg(collect_set("station").as("station"),
          collect_set("item").as("item"))

      //testdetailDF = spark.emptyDataFrame
      val count = testdetailDF.count()
      if(count == 0){
        println("count: 0 row")
        sys.exit
      }

      println("count: " + count + " row")
      //sample sql
      //      val sql = "select * from public.test_detail where station_name='TLEOL'" +
      //        "and product = 'TaiJi Base' and (array_length(array_positions(test_item, 'ProcPCClockSync^DResultInfo'), 1)>0" +
      //        "or array_length(array_positions(test_item, 'ProcPCClockSync^DResultInfo2'), 1)>0)"
      //      IoUtils.getDfFromCockroachdb(spark, sql)


      //test value
      //user_profile->'first_name', user_profile->'location'
      testdetailDF = testdetailDF.withColumn("selectSql",
        genTestDetailItemSelectSQL(lit("test_value"), col("item")))
      testdetailDF.select("selectSql").show(false)
      //testdetail filter sql
      var testdetailFilterColumnStr = "sn,build_name,build_description,unit_number,station_id,test_status,test_starttime," +
        "test_endtime,list_of_failure,list_of_failure_detail,test_phase,machine_id,factory_code,floor,line_id," +
        "create_time,update_time,station_name,start_date,product,test_version,test_value,"

      testdetailFilterColumnStr = testdetailFilterColumnStr + testdetailDF.select("selectSql").first().mkString("").toString()
      var selectSql = "select " + testdetailFilterColumnStr + " from test_detail"

      testdetailDF = testdetailDF.withColumn("whereSql",
        genTestDetailWhereSQL(col("product"), col("station"), col("item")))

      //撈測試結果細表的條件
      val selectSqlList = testdetailDF.select("whereSql").map(_.getString(0)).collect.toList
      val testDeailResultDf = IoUtils.getDfFromCockroachdb(spark, selectSqlList,
        testdetailFilterColumnStr, "whereSql", selectSql)

      //group by sn, staion_name, order by test_starttime
      val wSpecAsc = Window.partitionBy(col("sn"), col("station_name"))
        .orderBy(asc("test_starttime"))

      val wSpecDesc = Window.partitionBy(col("sn"), col("station_name"))
        .orderBy(desc("test_starttime"))

      var testDeailResultGroupByFirstDf = testDeailResultDf
        .withColumn("asc_rank", rank().over(wSpecAsc))
        .withColumn("desc_rank", rank().over(wSpecDesc))
        .where($"asc_rank".equalTo(1).or($"desc_rank".equalTo(1)))
        .withColumn("value_rank", getFirstOrLastRow($"asc_rank", $"desc_rank"))

      //以每個dataset, 收斂成一個工站資訊
      val station_name_str ="station_name"
      val station_info_str ="station_info"
      val item_info_str ="item_info"

      testDeailResultGroupByFirstDf = testdetailGroupByProductIdDF.withColumnRenamed("product", "product_dataset")
        .join(testDeailResultGroupByFirstDf, col("product_dataset").equalTo(col("product"))
        .and(array_contains($"station", $"station_name")), "left")
      testDeailResultGroupByFirstDf.show(false)

      val stationInfoStr = "build_description,unit_number,station_id,test_status,test_starttime,list_of_failure,test_version"
      var map = Map[String, String]()
      var list = List[String]()
      stationInfoStr.split(",").foreach(attr=>{
        var attrStr = attr+"_str"
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumn(attrStr, genStaionJsonFormat(col(station_name_str), lit(attr), col(attr)))
        map = map + (attrStr -> "collect_set")
        list = list :+ "collect_set(" + attrStr + ")"
      })

      //以每個dataset, 收斂成一個測項資訊
      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(item_info_str,
        genItemInfo($"station_name", $"item", $"test_value"))

      map = map + (item_info_str-> "collect_set")

      //group by 並收斂工站與測項資訊
      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.groupBy("id", "name", "product", "sn", "value_rank")
          .agg(map)
          .withColumn(station_info_str, array())

      list.foreach(ele => {
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumn(station_info_str, genStaionInfo(col(station_info_str), col(ele)))
          .drop(ele) //刪除collect_set的多個工站資訊欄位
      })

      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
       .withColumn(station_info_str, transferArrayToString(col(station_info_str)))
       .withColumn(item_info_str, transferArrayToString(col("collect_set("+item_info_str+")")))
       .drop(col("collect_set(item_info)"))
       .withColumn("sn_type", lit("unit_sn"))


      //組裝主表撈出sn對應的wo
      val snList = testDeailResultGroupByFirstDf.select("sn").dropDuplicates().map(_.getString(0)).collect.toList
      //TODO for ippd test_detail sn要取前十碼才對的上part_master sn
      val partMasterPredicates = Array[String]("sn in (" + snList.map(s => "'" + s.substring(0, 10) + "'").mkString(",") + ")")
      println("sn in (" + snList.map(s => "'" + s.substring(0, 10) + "'").mkString(",") + ")")

      var partMasterDf = IoUtils.getDfFromCockroachdb(spark, configLoader.getString("log_prop", "wip_table"), partMasterPredicates)

      val woList = partMasterDf.select("wo").dropDuplicates().map(_.getString(0)).collect.toList
      val woPredicates = Array[String]("wo in (" + woList.map(s => "'" + s.substring(3, s.length) + "'").mkString(",") + ")")
      //TODO for ippd part_master wo要取後三碼才對的上worker_order wo
      println("wo in (" + woList.map(s => "'" + s.substring(3, s.length) + "'").mkString(",") + ")")

      var woDf = IoUtils.getDfFromCockroachdb(spark, configLoader.getString("log_prop", "wo_table"), woPredicates)

      //只撈關鍵物料的component
      val componentList = datasetDf.select("component").dropDuplicates().map(_.getString(0)).collect.toList
      val configList = woDf.select("config").dropDuplicates().map(_.getString(0)).collect.toList
      val configPredicates = Array[String]("config in (" + configList.map(s => "'" + s + "'").mkString(",") + ") " +
        "and component in (" +   componentList.map(s => "'" + s + "'").mkString(",") + ")")
      val partsnPredicates = Array[String]("partsn in (" + configList.map(s => "'" + s + "'").mkString(",") + ")")

      println("config in (" + configList.map(s => "'" + s + "'").mkString(",") + ")")

      var partDetailDf = IoUtils.getDfFromCockroachdb(spark, configLoader.getString("log_prop", "wip_parts_table"), partsnPredicates)
          .dropDuplicates("partsn", "cust_part", "vendor_code", "date_code")
      partDetailDf.show(false)

      var comConfigDf = IoUtils.getDfFromCockroachdb(spark, configLoader.getString("log_prop", "mat_table"), configPredicates)
        .select("config","vendor", "hhpn", "oempn", "component", "component_type", "input_qty")
      val componentInfoStr = "vendor,hhpn,oempn,component_type,input_qty"
      val conponent_str ="component"
      val conponent_info_str ="component_info"
      //以每個dataset, 收斂成一個關鍵物料資訊
      //comConfigDf = comConfigDf.withColumn(conponent_str, col("component"))
      var comConfigMap = Map[String, String]()
      var comConfigList = List[String]()
      componentInfoStr.split(",").foreach(attr=>{
        var attrStr = attr+"_str"
        comConfigDf = comConfigDf
          .withColumn(attrStr, genStaionJsonFormat(col(conponent_str), lit(attr), col(attr)))
        comConfigMap = comConfigMap + (attrStr -> "collect_set")
        comConfigList = comConfigList :+ "collect_set(" + attrStr + ")"
      })

      var datasetComponentDF = testDeailResultGroupByFirstDf.select("id", "sn").dropDuplicates()
          .join(testdetailGroupByProductIdDF.select("id", "component").dropDuplicates(), Seq("id"))
          .withColumn("component", explode(col("component")))
          .join(comConfigDf, Seq("component"), "left")
      datasetComponentDF.show(false)

      //group by 並收斂關鍵物料資訊
      datasetComponentDF = datasetComponentDF.groupBy("id", "sn", "config")
        .agg(comConfigMap)
        .withColumn(conponent_info_str, array())

      comConfigList.foreach(ele => {
        datasetComponentDF = datasetComponentDF
          .withColumn(conponent_info_str, genStaionInfo(col(conponent_info_str), col(ele)))
          .drop(ele) //刪除collect_set的多個工站資訊欄位
      })

      datasetComponentDF.show(false)

      partMasterDf = partMasterDf.dropDuplicates("sn", "wo")
        //TODO for ippd part_master wo要取後三碼開始才對的上worker_order wo
        .withColumn("wo", expr("substring(wo, 4, length(wo))"))

      woDf = partMasterDf.join(woDf, Seq("wo"), "left")
        .join(partDetailDf, Seq("id"), "left")
        //.select("wo", "wo_type", "plant_code", "vendor_pn", "hhpn", "plan_qty", "config", "build_name","vendor_code","date_code")
        .select("wo", "wo_type", "plant_code", "plan_qty", "config", "build_name","vendor_code","date_code")
      woDf.show(false)
      //join 工單與關鍵
      datasetComponentDF = woDf.join(datasetComponentDF, Seq("config"), "left") //left

      datasetComponentDF.show(false)

      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
        //TODO for ippd test_detail sn要取前十碼才對的上part_master sn
        //.withColumn("sn", substring($"sn", 0, 10))
        .join(datasetComponentDF, Seq("id", "sn"), "left")
        .withColumn(conponent_info_str, transferArrayToString(col(conponent_info_str)))

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
        "`wo` varchar(50) DEFAULT NULL,"+
        "`wo_type` varchar(50) DEFAULT NULL,"+
        "`plant_code` varchar(50) DEFAULT NULL,"+
        //"`vendor_pn` varchar(50) DEFAULT NULL,"+
        //"`hhpn` varchar(50) DEFAULT NULL,"+
        "`plan_qty` varchar(50) DEFAULT NULL,"+
        "`config` varchar(50) DEFAULT NULL,"+
        "`build_name` varchar(50) DEFAULT NULL,"+
        "`vendor_code` varchar(50) DEFAULT NULL,"+
        "`date_code` varchar(50) DEFAULT NULL,"+
        //"`build_id` varchar(50) DEFAULT NULL,"+
        //"`config` varchar(50) DEFAULT NULL,"+ //configs?
        //"`color` varchar(500) DEFAULT NULL,"+ //TODO
        //"`component_type` varchar(500) DEFAULT NULL,"+ //category?
        //"`build_date` varchar(500) DEFAULT NULL,"+
        //"`input_qty` varchar(500) DEFAULT NULL,"+ //input_qty? //TODO
        //"`side/line` varchar(500) DEFAULT NULL,"+ //?
        //"`shift/side` varchar(500) DEFAULT NULL,"+ //?
//      vendor, oem_pn, component_config, sn //hhpn?
        "`component_info` json DEFAULT NULL,"+
        "PRIMARY KEY (`data_set_id`,`product`,`sn`,`value_rank`)"+
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"


      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
        .withColumnRenamed("name","data_set_name")
        .withColumnRenamed("id","data_set_id")

      val datasetIdDF = testDeailResultGroupByFirstDf.select("data_set_id").dropDuplicates("data_set_id")
      val idList = datasetIdDF.map(_.getString(0)).collect.toList
      for (id <- idList) {
        //drop dataset bigtable
        val dropSql = "DROP TABLE `data_set_bigtable@"+id+"`"
        mariadbUtils.execSqlToMariadb(dropSql)
        //truncate dataset bigtable schema
        val createSql = "CREATE TABLE `data_set_bigtable@"+id+"` ("+ schema
        mariadbUtils.execSqlToMariadb(createSql)
        //insert 大表資料
        mariadbUtils.saveToMariadb(testDeailResultGroupByFirstDf.filter(col("data_set_id").equalTo(id)),
          "`data_set_bigtable@"+id+"`", numExecutors)
        //update dataset 設定的欄位
        val updateSql = "UPDATE data_set_setting"+" SET bt_name='data_set_bigtable@"+id+"'," +
        " bt_create_time = COALESCE(bt_create_time, '"+jobStartTime+"')," +
        " bt_last_time = '" + jobStartTime + "'," +
        " bt_next_time = '" + nextExcuteTime + "'" +
        " WHERE id = " + id
        mariadbUtils.execSqlToMariadb(updateSql)

      }

    } catch {
      case ex: FileNotFoundException => {
        // ex.printStackTrace()
        println("===> FileNotFoundException !!!")
      }
    }
  }

}
