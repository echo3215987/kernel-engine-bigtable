package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale}

import com.foxconn.iisd.bd.rca.SparkUDF._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{regexp_replace, _}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel


object XWJBigtableNew {

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
    val datasetColumnStr = configLoader.getString("dataset", "setting_col")

    //testdetail filter sql
    val testdetailFilterColumnStr = configLoader.getString("log_prop", "test_detail_filter_col")

    val stationInfoColumn = configLoader.getString("dataset", "station_info_col")

    val itemInfoColumn = configLoader.getString("dataset", "item_info_col")

    val masterTable = configLoader.getString("log_prop", "wip_table")

    val detailTable = configLoader.getString("log_prop", "wip_parts_table")

    val partDetailColumnStr = configLoader.getString("dataset", "part_detail_col")

    val woTable = configLoader.getString("log_prop", "wo_table")

    val comTable = configLoader.getString("log_prop", "mat_table")

    val componentInfoColumn = configLoader.getString("dataset", "component_info_col")

    val datasetColumnNames = configLoader.getString("dataset", "bigtable_datatype_col")

    val datasetPath = configLoader.getString("minio_log_path", "dataset_path")

    val datatypeTable = configLoader.getString("dataset", "bigtable_datatype_table")

    val productItemSpecTable = configLoader.getString("dataset", "product_item_spec_table")


    //group by sn, staion_name, order by test_starttime
//    val wSpecTestDetailAsc = Window.partitionBy(col("sn"), col("station_name"))
//      .orderBy(asc("test_starttime"))
//
//    val wSpecTestDetailDesc = Window.partitionBy(col("sn"), col("station_name"))
//      .orderBy(desc("test_starttime"))

    //group by sn, order by scantime asc, 取第一筆
    val wSpecPartMasterAsc = Window.partitionBy(col("sn"))
      .orderBy(asc("scantime"))

    //group by part, order by scantime asc, 取第一筆
    val wSpecDetailAsc = Window.partitionBy(col("part"))
      .orderBy(asc("scantime"))

    ///////////
    //載入資料//
    ///////////

    try {

      //先讀dataset setting table
      val mariadbUtils = new MariadbUtils()


//      val datasetColumnStr = "id,name,product,bt_name,bt_create_time,bt_last_time,bt_next_time,effective_start_date,effective_end_date,component,item,station"
//      val datasetTableStr = "data_set_setting"
//
      val datasetSql = "select setting.id, setting.name, setting.product, setting.bt_name, setting.bt_create_time, " +
        "setting.bt_last_time, setting.bt_next_time, setting.effective_start_date, " +
        "setting.effective_end_date, part.component, item.item, item.station " +
        "from data_set_setting setting, data_set_station_item item, data_set_part part " +
        "where setting.id=item.dss_id and setting.id=part.dss_id " +
        "and setting.effective_start_date<='" + executeTime + "' "+
        "and setting.effective_end_date>='" + executeTime + "' "

      val datasetDf = mariadbUtils.execSqlToMariadbToDf(spark, datasetSql, datasetColumnStr)
        .filter($"item".isNotNull.and($"station".isNotNull))

      val datasetGroupByProductIdDF = datasetDf.groupBy("product", "id", "name")
        .agg(collect_set("station").as("station"),
        collect_set("item").as("item"),
          collect_set("component").as("component"))

//datasetGroupByProductIdDF.select("product", "id", "station", "item", "component").show(false)

      val datasetGroupByProductIdList = datasetGroupByProductIdDF.select("product", "id", "station", "item", "component").collect.toList

      //依每個資料集id建大表
      for(row <- datasetGroupByProductIdList){
        val id = row.getAs[String]("id")
        var currentDatasetDf = datasetGroupByProductIdDF.where("id='" + id + "'")
currentDatasetDf.show(false)
        val product = currentDatasetDf.select("product").map(_.getString(0)).collect().mkString("")
        val stationList = currentDatasetDf.selectExpr("explode(station)").dropDuplicates().map(_.getString(0)).collect.toList
        val itemList = currentDatasetDf.selectExpr("explode(item)").dropDuplicates().map(_.getString(0)).collect.toList
//      只撈出要的item
//      var selectSql = IoUtils.genTestDetailItemSelectSQL("test_value", r.getAs("item"))
//      val selectSql = "select " + testdetailFilterColumnStr + " from test_detail"
        val whereSql = IoUtils.genTestDetailWhereSQL(row.getAs("product"), row.getAs("station"))//, row.getAs("item")
        val sql = "select " + testdetailFilterColumnStr.split(",").map(col=> "t2." + col).mkString(",") + " from test_detail as t2, (select sn, station_name, agg_function(test_starttime) as test_starttime from test_detail" + whereSql + " group by sn, station_name) as t1 " +
          "where t2.sn=t1.sn and t2.station_name = t1.station_name and t1.test_starttime=t2.test_starttime"
        println(row)
        println(sql)

        //撈測試結果細表的條件
        val aggFunction = List("min", "max")
        val aggMap = Map("min"->"first","max"->"last")
        var testDeailResultGroupByFirstDf = spark.emptyDataFrame
        for(agg <- aggFunction){
          val rank = aggMap.apply(agg)
          val tempDf = IoUtils.getDfFromCockroachdb(spark, testdetailFilterColumnStr, sql.replace("agg_function", agg))
            .withColumn("value_rank", lit(rank))
            .persist(StorageLevel.DISK_ONLY)
          if(testDeailResultGroupByFirstDf.isEmpty)
            testDeailResultGroupByFirstDf = tempDf
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.union(tempDf)
        }

//        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
//          .withColumn("asc_rank", rank().over(wSpecTestDetailAsc))
//          .withColumn("desc_rank", rank().over(wSpecTestDetailDesc))
//          .where($"asc_rank".equalTo(1).or($"desc_rank".equalTo(1)))
//          .withColumn("value_rank", getFirstOrLastRow($"asc_rank", $"desc_rank"))
//          .withColumn("value_rank", explode($"value_rank"))

        //以每個dataset, 收斂成一個工站資訊
        val station_name_str ="station_name"
        val item_str ="item"
        val station_info_str ="station_info"
        val item_info_str ="item_info"

        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .join(currentDatasetDf, Seq("product"), "left")

        var map = Map[String, String]()
        var list = List[String]()
        stationInfoColumn.split(",").foreach(attr => {
          val attrStr = attr + "_str"
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
            .withColumn(attrStr, genStaionJsonFormat(col(station_name_str), lit(attr), col(attr)))
          map = map + (attrStr -> "collect_set")
          list = list :+ "collect_set(" + attrStr + ")"
        })

        var itemInfoList = List[String]()
        //以每個dataset, 收斂成一個測項資訊
        itemInfoColumn.split(",").foreach(attr => {
          val attrStr = attr + "_str"
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(attrStr,
            genItemJsonFormat(col(station_name_str), col(item_str), col(attr), lit(attr)))
          map = map + (attrStr -> "collect_set")
          itemInfoList = itemInfoList :+ "collect_set(" + attrStr + ")"
        })

        //group by 並收斂工站與測項資訊
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.groupBy("id", "sn", "value_rank")
          .agg(map)
          .withColumn(station_info_str, array())
          .withColumn(item_info_str, array())

        list.foreach(ele => {
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
            .withColumn(station_info_str, genInfo(col(station_info_str), col(ele)))
            .drop(ele) //刪除collect_set的多個工站資訊欄位
        })

        itemInfoList.foreach(ele => {
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
            .withColumn(item_info_str,
              genInfo(col(item_info_str), col(ele)))
            .drop(ele) //刪除collect_set的多個工站資訊欄位
        })

        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumn(station_info_str, transferArrayToString(col(station_info_str)))
          .withColumn(item_info_str, transferArrayToString(col(item_info_str)))
          .join(currentDatasetDf.select("id", "name", "product", "component"), Seq("id"), "left")
          //      新增sn_type, 紀錄是整機還是非整機測試(比對測試明細sn vs unit_number), 目前先維持定值, 之後有需要分整機或非整機測試再處理
          .withColumn("sn_type", lit("wip"))//unit

        //串工單數據與關鍵物料
        //組裝主表撈出sn對應的wo
        val snList = testDeailResultGroupByFirstDf.select("sn").dropDuplicates().map(_.getString(0)).collect.toList
        val snCondition = "sn in (" + snList.map(s => "'" + s + "'").mkString(",") + ")"
        println(snCondition)

        val masterWhereSql = "select id,floor,wo,scantime,sn from " + masterTable + " where " + snCondition
        var partMasterDf = IoUtils.getDfFromCockroachdb(spark, masterWhereSql, numExecutors)

        partMasterDf = partMasterDf
          .withColumn("rank", rank().over(wSpecPartMasterAsc))
          .where($"rank".equalTo(1))

        val partMasterIdList = partMasterDf.select("id").dropDuplicates().map(_.getString(0)).collect.toList

        val woList = partMasterDf.select("wo").dropDuplicates().map(_.getString(0)).collect.toList
        val woCondition = "wo in (" + woList.map(s => "'" + s + "'").mkString(",") + ")"
        println(woCondition)

        val woWhereSql = "select wo,wo_type,plant_code,plan_qty,config,build_name,release_date from " + woTable + " where " + woCondition
        var woDf = IoUtils.getDfFromCockroachdb(spark, woWhereSql, numExecutors)

        //只撈關鍵物料的component
        val componentList = currentDatasetDf.selectExpr("explode(component)").dropDuplicates().map(_.getString(0)).collect.toList
        val configList = woDf.select("config").dropDuplicates().map(_.getString(0)).collect.toList

        val componentCondition = "config in (" + configList.map(s => "'" + s + "'").mkString(",") + ") " +
          "and component in (" + componentList.map(s => "'" + s + "'").mkString(",") + ")"
        println(componentCondition)

        val comWhereSql = "select config,vendor,hhpn,oempn,component,component_type,input_qty from " + comTable + " where " + componentCondition
        var comDf = IoUtils.getDfFromCockroachdb(spark, comWhereSql, numExecutors)

        //dataset選的關鍵物料
        val partDetailCondition = "id in (" + partMasterIdList.map(s => "'" + s + "'").mkString(",") + ") " +
          " and part in (" + componentList.map(s => "'" + s + "'").mkString(",") + ")"
        println(partDetailCondition)

        val partDetailWhereSql = "select id,partsn,scantime,vendor_code,date_code,part from " + detailTable + " where " + partDetailCondition
        var partDetailDf = IoUtils.getDfFromCockroachdb(spark, partDetailWhereSql, numExecutors)

        val partList = partDetailDf.select("part").dropDuplicates().map(_.getString(0)).collect.toList
        val partCondition = "part in (" + partList.map(s => "'" + s + "'").mkString(",") + ")"
        println(partCondition)
//        partsn,vendor_code,date_code,part
        val partDetailColumn = partDetailColumnStr.split(",")
        partDetailDf = partDetailDf
          .withColumn("rank", rank().over(wSpecDetailAsc))
          .where($"rank".equalTo(1))
          .selectExpr(partDetailColumn: _*)
          .withColumnRenamed("part", "component")

        comDf = comDf.join(partDetailDf, Seq("component"),"left")

        //以每個dataset, 收斂成一個關鍵物料資訊
//        val componentInfoStr = "vendor,hhpn,oempn,component_type,input_qty"
        val component_str ="component"
        val component_info_str ="component_info"

        var comConfigMap = Map[String, String]()
        var comConfigList = List[String]()
        componentInfoColumn.split(",").foreach(attr => {
          val attrStr = attr + "_str"
          comDf = comDf
            .withColumn(attrStr, genStaionJsonFormat(col(component_str), lit(attr), col(attr)))
          comConfigMap = comConfigMap + (attrStr -> "collect_set")
          comConfigList = comConfigList :+ "collect_set(" + attrStr + ")"
        })

        var datasetComponentDF = testDeailResultGroupByFirstDf.select("component").dropDuplicates()
//          testDeailResultGroupByFirstDf.select("sn", "component").dropDuplicates()
//            testDeailResultGroupByFirstDf.select("id","sn").dropDuplicates()
//          .join(currentDatasetDf.select("id","component").dropDuplicates(), Seq("id"))
          .withColumn("component", explode(col("component")))
          .join(comDf, Seq("component"), "left")

        //group by 並收斂關鍵物料資訊
        datasetComponentDF = datasetComponentDF.groupBy("config")
//          datasetComponentDF.groupBy("sn", "config")
          .agg(comConfigMap)
          .withColumn(component_info_str, array())

        comConfigList.foreach(ele => {
          datasetComponentDF = datasetComponentDF
            .withColumn(component_info_str, genInfo(col(component_info_str), col(ele)))
            .drop(ele) //刪除collect_set的多個工站資訊欄位
        })
//        datasetComponentDF.show(false)
        partMasterDf = partMasterDf
          .select("sn", "floor", "scantime", "wo")
          .dropDuplicates()

        //一個工單號或對到多個config?
        woDf = woDf.select("wo", "wo_type", "plant_code", "plan_qty", "config", "build_name", "release_date")
        //group by wo, order by release_date desc, 取第一筆
        val wSpecWoDesc = Window.partitionBy(col("wo"))
          .orderBy(desc("release_date"))
        woDf = woDf.withColumn("rank", rank().over(wSpecWoDesc))
          .where($"rank".equalTo(1))
          .drop("rank")
          .drop("release_date")
          .dropDuplicates("wo")
        woDf = partMasterDf.join(woDf, Seq("wo"), "left")
        woDf = woDf.join(datasetComponentDF, Seq("config"), "left")

//partMasterDf.show(false)
//woDf.show(false)

        //join 工單與關鍵物料
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .join(woDf, Seq("sn"), "left")
          .withColumn(component_info_str, transferArrayToString(col(component_info_str)))
          .drop("component")

        testDeailResultGroupByFirstDf.show(1, false)

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

        val datasetTableName = "`data_set_bigtable@"+id+"`"
        //drop dataset bigtable
        val dropSql = "DROP TABLE IF EXISTS " + datasetTableName
        mariadbUtils.execSqlToMariadb(dropSql)
        //truncate dataset bigtable schema
        val createSql = "CREATE TABLE " + datasetTableName + " ("+ schema
        mariadbUtils.execSqlToMariadb(createSql)

//      insert 大表資料
//          val datasetOutputDF = testDeailResultGroupByFirstDf.filter(col("data_set_id").equalTo(id))
        mariadbUtils.saveToMariadb(testDeailResultGroupByFirstDf, datasetTableName, numExecutors)
//      update dataset 設定的欄位
        val updateSql = "UPDATE data_set_setting"+" SET bt_name='"+datasetTableName.substring(1,datasetTableName.length-1)+"'," +
        " bt_create_time = COALESCE(bt_create_time, '"+jobStartTime+"')," +
        " bt_last_time = '" + jobStartTime + "'," +
        " bt_next_time = '" + nextExcuteTime + "'" +
        " WHERE id = " + id
        mariadbUtils.execSqlToMariadb(updateSql)

        //展開json欄位匯出csv提供客戶下載, 並將大表欄位儲存起來
        val jsonColumns = List(station_info_str, item_info_str, component_info_str)

//        var datasetOutputDF = testDeailResultGroupByFirstDf
        for(jsonInfo <- jsonColumns){
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(jsonInfo, regexp_replace(col(jsonInfo), "'", ""))
            .persist(StorageLevel.DISK_ONLY)
        }

        //記錄dataset大表欄位
        val datasetSchema = IoUtils.createStringSchema(datasetColumnNames)

        //station 與 component json固定資訊
        val jsonMap = Map(station_info_str -> stationInfoColumn, component_info_str -> componentInfoColumn)

        val dataTypeCondition = "product = '" + product + "'" + " and station_name in (" + stationList.map(s => "'" + s + "'").mkString(",")+")" +
        " and test_item in (" + itemList.map(s => "'" + s + "'").mkString(",")+")"
        val dataTypeSql = "select test_item,test_item_datatype from " + productItemSpecTable + " where " + dataTypeCondition
        val dataTypeDF = mariadbUtils.getDfFromMariadbWithQuery(spark, dataTypeSql, numExecutors)
        val dataTypeMap = dataTypeDF.select($"test_item", $"test_item_datatype").as[(String, String)].collect.toMap

        //展開json object, 存成csv
        var jsonColumnMapping = Map[String, String]()
        for(jsonInfo <- jsonColumns){
          val datasetColumnName = jsonInfo.replace("_info", "")
          currentDatasetDf = currentDatasetDf.withColumn(datasetColumnName, explode(col(datasetColumnName)))
          val infoList = currentDatasetDf.select(datasetColumnName).dropDuplicates().map(_.getString(0)).collect.toList
          var infoFielids = List[String]()
          if(jsonInfo.equals(item_info_str)){
            val stationInfo = currentDatasetDf.select("station").dropDuplicates().map(_.getString(0)).collect.mkString(",")
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

          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(jsonInfo, from_json(col(jsonInfo), schema))
            .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
          for (column <- infoFielids) {
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(column, col(jsonInfo).getItem(column))
          }
        }

        for(jsonInfo <- jsonColumns){
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.drop(jsonInfo)
        }

        testDeailResultGroupByFirstDf.repartition(1).write.option("header", "true")
          .mode("overwrite").csv(datasetPath + "/" + id)

        //存大表的欄位型態datatype到mysql
        val columnNames = testDeailResultGroupByFirstDf.columns
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
