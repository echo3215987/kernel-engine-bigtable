package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.foxconn.iisd.bd.rca.SparkUDF.{genInfo, genItemJsonFormat, genStaionJsonFormat, genTestDetailSelectSql, getFirstOrLastRow, transferArrayToString}
import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.Seq

object Bigtable{

    val mariadbUtils = new MariadbUtils()
    val datasetColumnStr = configLoader.getString("dataset", "setting_col")

    val testdetailTable = configLoader.getString("log_prop", "test_detail_table")

    val testdetailFilterColumn = configLoader.getString("log_prop", "test_detail_filter_col")

    val masterFilterColumn = configLoader.getString("log_prop", "wip_filter_col")

    val masterTable = configLoader.getString("log_prop", "wip_table")

    val detailFilterColumn = configLoader.getString("log_prop", "wip_parts_filter_col")

    val detailTable = configLoader.getString("log_prop", "wip_parts_table")

    val partDetailColumnStr = configLoader.getString("dataset", "part_detail_col")

    val woTable = configLoader.getString("log_prop", "wo_table")

    val comTable = configLoader.getString("log_prop", "mat_table")

    val stationInfoColumn = configLoader.getString("dataset", "station_info_col")

    val itemInfoColumn = configLoader.getString("dataset", "item_info_col")

    val componentInfoColumn = configLoader.getString("dataset", "component_info_col")

    val stationName = configLoader.getString("dataset", "station_name_str")

    val stationInfo = configLoader.getString("dataset", "station_info_str")

    val item = configLoader.getString("dataset", "item_str")

    val itemInfo = configLoader.getString("dataset", "item_info_str")

    val component = configLoader.getString("dataset", "component_str")

    val componentInfo = configLoader.getString("dataset", "component_info_str")

//    group by id, staion_name, order by test_starttime
    val wSpecTestDetailAsc = Window.partitionBy(col("id"), col("station_name"))
      .orderBy(asc("test_starttime"))

    val wSpecTestDetailDesc = Window.partitionBy(col("id"), col("station_name"))
      .orderBy(desc("test_starttime"))

    //group by wo, order by release_date desc, 取第一筆
    val wSpecWoDesc = Window.partitionBy(col("wo"))
      .orderBy(desc("release_date"))

    //group by sn, order by scantime asc, 取第一筆, 改到ke處理
//    val wSpecPartMasterAsc = Window.partitionBy(col("sn"))
//      .orderBy(asc("scantime"))

    //group by part, order by scantime asc, 取第一筆
    val wSpecDetailAsc = Window.partitionBy(col("part"))
      .orderBy(asc("scantime"))


  def createBigtable(spark: SparkSession, row: Row, currentDatasetDf: DataFrame,
                       currentDatasetStationItemDf: DataFrame, id: String,
                       jobStartTime: String, nextExcuteTime: String) ={
        import spark.implicits._
        val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt

//        currentDatasetDf.show()
//        currentDatasetStationItemDf.show()

        //只撈出該資料集distinct的item, 只有選的station與item
        val currentDatasetStationItemDF = currentDatasetStationItemDf.withColumn("selectSql",
            genTestDetailSelectSql(array(lit("test_value"), lit("test_item_result"), lit("test_item_result_detail")),
                col("item")))
//        currentDatasetStationItemDF.show()

        var testDeailResultGroupByFirstDf = spark.emptyDataFrame

        val aggFunction = List("min", "max")
        val aggMap = Map("min" -> "first", "max" -> "last")

        val currentDatasetStationItemList = currentDatasetStationItemDF.select("station_name", "selectSql").collect.toList
println("-----------------> select testdetail first / last -> start_time:" + new SimpleDateFormat(
  configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

        for(stationItemRow <- currentDatasetStationItemList){
            //撈測試結果細表的條件
//            val whereSql = IoUtils.genTestDetailWhereSQL(row.getAs("product"), stationItemRow.getAs("station_name"))
//            val testdetailSql = "select " + testdetailFilterColumn.split(",").map(col => "t2." + col).mkString(",") + "," +
//              stationItemRow.getAs("selectSql") +
//              " from " + testdetailTable + " as t2, " +
//          //收大產品把sn改成id
//              "(select id, station_name, agg_function(test_starttime) as test_starttime from " + testdetailTable + whereSql +
//              " group by id, station_name) as t1 " +
//              " where t2.id=t1.id and t2.station_name = t1.station_name and t1.test_starttime=t2.test_starttime"
//
//            for (agg <- aggFunction) {
//                val rank = aggMap.apply(agg)
//                val tempDf = IoUtils.getDfFromCockroachdb(spark, testdetailSql.replace("agg_function", agg), numExecutors)
//                  .withColumn("value_rank", lit(rank))
//                if (testDeailResultGroupByFirstDf.isEmpty)
//                    testDeailResultGroupByFirstDf = tempDf
//                testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.union(tempDf)
//            }
//            子查詢改用過濾初步條件後, 撈回來的資料用spark group by取第一筆在memory處理
              val whereSql = IoUtils.genTestDetailWhereSQL(row.getAs("product"), stationItemRow.getAs("station_name")) + " and id is not null"
              val testdetailSql = "select " + testdetailFilterColumn.split(",").mkString(",") + "," +
                        stationItemRow.getAs("selectSql") + " from " + testdetailTable + whereSql
              val tempDf = IoUtils.getDfFromCockroachdb(spark, testdetailSql, numExecutors)
              if (testDeailResultGroupByFirstDf.isEmpty)
                  testDeailResultGroupByFirstDf = tempDf
              else
                  testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.union(tempDf)

        }
//          取scantime第一筆或最後一筆
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
              .withColumn("asc_rank", rank().over(wSpecTestDetailAsc))
              .withColumn("desc_rank", rank().over(wSpecTestDetailDesc))
              .where($"asc_rank".equalTo(1).or($"desc_rank".equalTo(1)))
              .withColumn("value_rank", getFirstOrLastRow($"asc_rank", $"desc_rank"))
              .withColumn("value_rank", explode($"value_rank")).persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("-----------------> select testdetail first / last -> end_time:" + new SimpleDateFormat(
configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

      val fieldsColumnDataType = testDeailResultGroupByFirstDf.schema.fields
//      Cartridge大產品(id 改成 sn)
      testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn("sn", col("id"))
//testDeailResultGroupByFirstDf.show(1, false)
        //紀錄part_master資訊與part_detail line欄位
        //先調整part_master的id欄位名稱
        var partMasterDf = testDeailResultGroupByFirstDf
          .drop(col("floor")) //刪除測試樓層
          .dropDuplicates("sn")
          .withColumnRenamed("scan_floor", "floor") //更改組裝樓層名稱scan_floor -> floor
          .select("sn", "wo", "id", "scantime", "floor", "line")

        //以每個dataset, 收斂成一個工站資訊
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .drop("wo", "id", "scantime", "floor", "line") //刪除組裝主表與細表的相關欄位
          .join(currentDatasetDf, Seq("product"), "left")
          .join(currentDatasetStationItemDF.select("station_name", "item"), Seq("station_name"), "left")
          .persist(StorageLevel.MEMORY_AND_DISK_SER)
        var map = Map[String, String]()
        var list = List[String]()
        stationInfoColumn.split(",").foreach(attr => {
            val attrStr = attr + "_str"
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
              .withColumn(attrStr, genStaionJsonFormat(col(stationName), lit(attr), col(attr)))
            map = map + (attrStr -> "collect_set")
            list = list :+ "collect_set(" + attrStr + ")"
        })

        var itemInfoList = List[String]()
        //以每個dataset, 收斂成一個測項資訊
        itemInfoColumn.split(",").foreach(attr => {
            val attrStr = attr + "_str"
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(attrStr,
                genItemJsonFormat(col(stationName), col(item), col(attr), lit(attr)))
            map = map + (attrStr -> "collect_set")
            itemInfoList = itemInfoList :+ "collect_set(" + attrStr + ")"
        })

        //group by 並收斂工站與測項資訊
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.groupBy("id", "sn", "value_rank")
          .agg(map)
          .withColumn(stationInfo, array())
          .withColumn(itemInfo, array())

        list.foreach(ele => {
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
              .withColumn(stationInfo, genInfo(col(stationInfo), col(ele)))
              .drop(ele) //刪除collect_set的多個工站資訊欄位
        })

        itemInfoList.foreach(ele => {
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
              .withColumn(itemInfo,
                  genInfo(col(itemInfo), col(ele)))
              .drop(ele) //刪除collect_set的多個工站資訊欄位
        })

        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumn(stationInfo, transferArrayToString(col(stationInfo)))
          .withColumn(itemInfo, transferArrayToString(col(itemInfo)))
          .join(currentDatasetDf.select("id", "name", "product", "component"), Seq("id"), "left")
          //新增sn_type, 紀錄是整機還是非整機測試(比對測試明細sn vs unit_number), 目前先維持定值, 之後有需要分整機或非整機測試再處理
          .withColumn("sn_type", lit("wip")) //unit

//v2改到xwj ke處理
        //串工單數據與關鍵物料
        //組裝主表撈出sn對應的wo
//        val snList = testDeailResultGroupByFirstDf.select("sn").dropDuplicates().map(_.getString(0)).collect.toList
//        val snCondition = "sn in (" + snList.map(s => "'" + s + "'").mkString(",") + ")"
//        println(snCondition)


//println("-----------------> select part table (first scantime) where sn, product start_time:" + new SimpleDateFormat(
//  configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

//        val masterSql = "select " + masterFilterColumn.split(",").map(col => "t2." + col).mkString(",") + " from " + masterTable + " as t2, " +
//          "(select sn, product, min(scantime) as scantime from " + masterTable + " where " + snCondition + " group by sn, product) as t1 " +
//          "where t2.sn=t1.sn and t2.product = t1.product and t1.scantime=t2.scantime"
//        var partMasterDf = IoUtils.getDfFromCockroachdb(spark, masterSql, numExecutors)

//println("-----------------> select part table (first scantime) where sn, product end_time:" + new SimpleDateFormat(
//configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

        val woList = partMasterDf
            .filter(col("wo").notEqual("N/A"))
            .select("wo").dropDuplicates().map(_.getString(0)).collect.toList
        var woDf = spark.emptyDataFrame
        var comDf = spark.emptyDataFrame
        var partDetailDf = spark.emptyDataFrame
        if(woList.size > 0){

          println("-----------------> select wo, start_time:" + new SimpleDateFormat(
            configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

          val woCondition = "wo in (" + woList.map(s => "'" + s + "'").mkString(",") + ")"
          //        println(woCondition)
          val woWhereSql = "select wo,wo_type,plant_code,plan_qty,config,build_name,release_date from " + woTable + " where " + woCondition
          woDf = IoUtils.getDfFromCockroachdb(spark, woWhereSql, numExecutors)

          println("-----------------> select wo, end_time:" + new SimpleDateFormat(
            configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

          println("-----------------> select config_component, start_time:" + new SimpleDateFormat(
            configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))
          //只撈關鍵物料的component
          val configList = woDf.select("config").dropDuplicates().map(_.getString(0)).collect.toList
          val componentList = currentDatasetDf.selectExpr("explode(component)").dropDuplicates().map(_.getString(0)).collect.toList


          if(configList.size > 0 && componentList.size > 0){
            val componentCondition = "config in (" + configList.map(s => "'" + s + "'").mkString(",") + ") " +
              "and component in (" + componentList.map(s => "'" + s + "'").mkString(",") + ")"
            //        println(componentCondition)

            val comWhereSql = "select config,vendor,hhpn,oempn,component,component_type,input_qty from " + comTable + " where " + componentCondition
            comDf = IoUtils.getDfFromCockroachdb(spark, comWhereSql, numExecutors)
          }


          println("-----------------> select config_component, end_time:" + new SimpleDateFormat(
            configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

          println("-----------------> select part_sn, start_time:" + new SimpleDateFormat(
            configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))
          val partMasterIdList = partMasterDf.select("id").dropDuplicates().map(_.getString(0)).collect.toList

          if(partMasterIdList.size>0 && componentList.size>0){
            //dataset選的關鍵物料
            val partDetailCondition = "id in (" + partMasterIdList.map(s => "'" + s + "'").mkString(",") + ") " +
              " and part in (" + componentList.map(s => "'" + s + "'").mkString(",") + ")"

            val partDetailWhereSql = "select " + detailFilterColumn.split(",").mkString(",") + " from " + detailTable + " where " + partDetailCondition
            partDetailDf = IoUtils.getDfFromCockroachdb(spark, partDetailWhereSql, numExecutors)

            //          val detailSql = "select "  + detailFilterColumn.split(",").map(col => "t2." + col).mkString(",") + " from " + detailTable + " as t2, " +
            //            "(select id, part, min(scantime) as scantime from " + detailTable + " where " + partDetailCondition + " group by id, part) as t1 " +
            //            "where t2.id=t1.id and t2.part = t1.part and t1.scantime=t2.scantime"
            //          var partDetailDf = IoUtils.getDfFromCockroachdb(spark, detailSql, numExecutors)

            //取第一筆scantime組裝細表資料
            //partsn,vendor_code,date_code,part
            val partDetailColumn = partDetailColumnStr.split(",")
            partDetailDf = partDetailDf
              .withColumn("rank", rank().over(wSpecDetailAsc))
              .where($"rank".equalTo(1))
              .selectExpr(partDetailColumn: _*)
              .withColumnRenamed("part", "component")
          }

          println("-----------------> select part_sn, end_time:" + new SimpleDateFormat(
            configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

        }

        partMasterDf = partMasterDf
          .select("sn", "floor", "scantime", "wo", "line")
          .dropDuplicates()

        if(comDf.isEmpty){
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
            .join(partMasterDf, Seq("sn"), "left")
            .withColumn(componentInfo, lit(null).cast(StringType))
            .drop("component")

        }else{
          comDf = comDf.join(partDetailDf, Seq("component"), "left")

          //以每個dataset, 收斂成一個關鍵物料資訊
          var comConfigMap = Map[String, String]()
          var comConfigList = List[String]()
          componentInfoColumn.split(",").foreach(attr => {
            val attrStr = attr + "_str"
            comDf = comDf
              .withColumn(attrStr, genStaionJsonFormat(col(component), lit(attr), col(attr)))
            comConfigMap = comConfigMap + (attrStr -> "collect_set")
            comConfigList = comConfigList :+ "collect_set(" + attrStr + ")"
          })

          var datasetComponentDF = testDeailResultGroupByFirstDf.select("component").dropDuplicates()
            .withColumn("component", explode(col("component")))
            .join(comDf, Seq("component"), "left")

          //group by 並收斂關鍵物料資訊
          datasetComponentDF = datasetComponentDF.groupBy("config")
            .agg(comConfigMap)
            .withColumn(componentInfo, array())

          comConfigList.foreach(ele => {
            datasetComponentDF = datasetComponentDF
              .withColumn(componentInfo, genInfo(col(componentInfo), col(ele)))
              .drop(ele) //刪除collect_set的多個工站資訊欄位
          })

          //一個工單號或對到多個config?
          woDf = woDf.select("wo", "wo_type", "plant_code", "plan_qty", "config", "build_name", "release_date")

          woDf = woDf.withColumn("rank", rank().over(wSpecWoDesc))
            .where($"rank".equalTo(1))
            .drop("rank")
            .drop("release_date")
            .dropDuplicates("wo")
          woDf = partMasterDf.join(woDf, Seq("wo"), "left")
          woDf = woDf.join(datasetComponentDF, Seq("config"), "left")

          //join 工單與關鍵物料
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
            .join(woDf, Seq("sn"), "left")
            .withColumn(componentInfo, transferArrayToString(col(componentInfo)))
            .drop("component")
        }

//        testDeailResultGroupByFirstDf.show(1, false)

        //create dataset bigtable schema
        val schema = "`data_set_name` varchar(200) Not NULL," +
          "`data_set_id` varchar(30) Not NULL," +
          "`product` varchar(50) Not NULL," +
          "`sn` varchar(100) Not NULL," +
          "`sn_type` varchar(100) DEFAULT NULL," +
          "`value_rank` varchar(30) Not NULL," +
          "`station_info` json Not NULL," +
          "`item_info` json Not NULL," +
          "`floor` varchar(50) DEFAULT NULL," + //組裝樓層
          "`line` varchar(50) DEFAULT NULL," + //組裝線別
          "`scantime` datetime DEFAULT NULL," +
          "`wo` varchar(50) DEFAULT NULL," +
          "`wo_type` varchar(50) DEFAULT NULL," +
          "`plant_code` varchar(50) DEFAULT NULL," +
          "`plan_qty` varchar(50) DEFAULT NULL," +
          "`config` varchar(50) DEFAULT NULL," +
          "`build_name` varchar(50) DEFAULT NULL," +
          "`component_info` json DEFAULT NULL," +
          "PRIMARY KEY (`data_set_id`,`product`,`sn`,`value_rank`)" +
          ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumnRenamed("name", "data_set_name")
          .withColumnRenamed("id", "data_set_id")
//          .persist(StorageLevel.MEMORY_AND_DISK_SER)
          .persist(StorageLevel.DISK_ONLY)

println("-----------------> drop and insert bigtable: " + id + ", start_time:" + new SimpleDateFormat(
  configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

        val datasetTableName = "`data_set_bigtable@" + id + "`"
        //drop dataset bigtable
        val dropSql = "DROP TABLE IF EXISTS " + datasetTableName
        mariadbUtils.execSqlToMariadb(dropSql)

        Thread.sleep(500)
        //drop後, 先等五秒再create

        //truncate dataset bigtable schema
        val createSql = "CREATE TABLE " + datasetTableName + " (" + schema
        mariadbUtils.execSqlToMariadb(createSql)


          //insert 大表資料  -> 改成 df.write
          println("-----------------> insert data" + id + ", start_time:" + new SimpleDateFormat(
            configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))
          println("numExecutors:" + numExecutors)
          mariadbUtils.saveToMariadb(testDeailResultGroupByFirstDf, datasetTableName, numExecutors)
          println("-----------------> insert data" + id + ", end_time:" + new SimpleDateFormat(
            configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))

//          val mariadbConnectionProperties = new Properties()
//
//          mariadbConnectionProperties.put(
//            "user", "root"
//          )
//
//          mariadbConnectionProperties.put(
//            "password", "123456"
//          )
//          println(numExecutors)
//          val url = "jdbc:mysql:loadbalance://10.57.232.173:3306/testdb?useSSL=false&serverTimezone=Asia/Taipei&useUnicode=true&characterEncoding=UTF-8"
//          println("test connections--start")
//          testDeailResultGroupByFirstDf.write
//            .mode(SaveMode.Append)
//            .option("numPartitions", numExecutors)
//            .option("dbtable", datasetTableName)
//            .jdbc(url, datasetTableName, mariadbConnectionProperties)
//          println("test connections--end")


println("-----------------> drop and insert bigtable: " + id + ", end_time:" + new SimpleDateFormat(
  configLoader.getString("summary_log_path", "job_fmt")).format(new Date().getTime()))
       (testDeailResultGroupByFirstDf, fieldsColumnDataType)

    }
}
