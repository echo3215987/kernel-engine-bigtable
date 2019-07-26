package com.foxconn.iisd.bd.rca

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.foxconn.iisd.bd.rca.SparkUDF.{genInfo, genItemJsonFormat, genStaionJsonFormat, transferArrayToString, genTestDetailSelectSql}
import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
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

    def createBigtable(spark: SparkSession, row: Row, currentDatasetDf: DataFrame,
                       currentDatasetStationItemDf: DataFrame, id: String,
                       jobStartTime: String, nextExcuteTime: String): DataFrame ={
        import spark.implicits._
        val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt

        currentDatasetDf.show()
        currentDatasetStationItemDf.show()

        //只撈出該資料集distinct的item station跟item乘開
//        val testItemList = List("test_value", "test_item_result", "test_item_result_detail")
//        val testItemJsonbSql = testItemList.map( testItem => IoUtils.genTestDetailItemSelectSQL(testItem, row.getAs("item"))).mkString(",")
//        println(testItemJsonbSql)
//
//        val whereSql = IoUtils.genTestDetailWhereSQL(row.getAs("product"), row.getAs("station"), row.getAs("item"))
//        val testdetailSql = "select " + testdetailFilterColumn.split(",").map(col => "t2." + col).mkString(",") + "," + testItemJsonbSql +
//          " from " + testdetailTable + " as t2, " +
//          "(select sn, station_name, agg_function(test_starttime) as test_starttime from " + testdetailTable + whereSql + " group by sn, station_name) as t1 " +
//          "where t2.sn=t1.sn and t2.station_name = t1.station_name and t1.test_starttime=t2.test_starttime"
//        println(row)
//        println(testdetailSql)

        //撈測試結果細表的條件
//        val aggFunction = List("min", "max")
//        val aggMap = Map("min" -> "first", "max" -> "last")
//        var testDeailResultGroupByFirstDf = spark.emptyDataFrame
//        for (agg <- aggFunction) {
//            val rank = aggMap.apply(agg)
//              val tempDf = IoUtils.getDfFromCockroachdb(spark, testdetailSql.replace("agg_function", agg), numExecutors)
//              .withColumn("value_rank", lit(rank))
//              .persist(StorageLevel.DISK_ONLY)
//            if (testDeailResultGroupByFirstDf.isEmpty)
//                testDeailResultGroupByFirstDf = tempDf
//            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.union(tempDf)
//        }

        //只撈出該資料集distinct的item, 只有選的station與item
        val currentDatasetStationItemDF = currentDatasetStationItemDf.withColumn("selectSql",
            genTestDetailSelectSql(array(lit("test_value"), lit("test_item_result"), lit("test_item_result_detail")),
                col("item")))
        currentDatasetStationItemDF.show()

        var testDeailResultGroupByFirstDf = spark.emptyDataFrame

        val aggFunction = List("min", "max")
        val aggMap = Map("min" -> "first", "max" -> "last")

        val currentDatasetStationItemList = currentDatasetStationItemDF.select("station_name", "selectSql").collect.toList
        for(stationItemRow <- currentDatasetStationItemList){
            //撈測試結果細表的條件
            val whereSql = IoUtils.genTestDetailWhereSQL(row.getAs("product"), stationItemRow.getAs("station_name"))
            val testdetailSql = "select " + testdetailFilterColumn.split(",").map(col => "t2." + col).mkString(",") + "," +
              stationItemRow.getAs("selectSql") +
              " from " + testdetailTable + " as t2, " +
              "(select sn, station_name, agg_function(test_starttime) as test_starttime from " + testdetailTable + whereSql + " group by sn, station_name) as t1 " +
              "where t2.sn=t1.sn and t2.station_name = t1.station_name and t1.test_starttime=t2.test_starttime"
            println(row)
            println(testdetailSql)

            for (agg <- aggFunction) {
                val rank = aggMap.apply(agg)
                val tempDf = IoUtils.getDfFromCockroachdb(spark, testdetailSql.replace("agg_function", agg), numExecutors)
                  .withColumn("value_rank", lit(rank))
                  .persist(StorageLevel.DISK_ONLY)
                if (testDeailResultGroupByFirstDf.isEmpty)
                    testDeailResultGroupByFirstDf = tempDf
                testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.union(tempDf)
            }
        }

testDeailResultGroupByFirstDf.show(10,false)

        //以每個dataset, 收斂成一個工站資訊
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .join(currentDatasetDf, Seq("product"), "left")
          .join(currentDatasetStationItemDF.select("station_name", "item"), Seq("station_name"), "left")

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
          //      新增sn_type, 紀錄是整機還是非整機測試(比對測試明細sn vs unit_number), 目前先維持定值, 之後有需要分整機或非整機測試再處理
          .withColumn("sn_type", lit("wip")) //unit

        //串工單數據與關鍵物料
        //組裝主表撈出sn對應的wo
        val snList = testDeailResultGroupByFirstDf.select("sn").dropDuplicates().map(_.getString(0)).collect.toList
        val snCondition = "sn in (" + snList.map(s => "'" + s + "'").mkString(",") + ")"
        println(snCondition)

        val masterSql = "select "  + masterFilterColumn.split(",").map(col => "t2." + col).mkString(",") + " from " + masterTable + " as t2, " +
          "(select sn, product, min(scantime) as scantime from " + masterTable + " where " + snCondition + " group by sn, product) as t1 " +
          "where t2.sn=t1.sn and t2.product = t1.product and t1.scantime=t2.scantime"
        var partMasterDf = IoUtils.getDfFromCockroachdb(spark, masterSql, numExecutors)

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

        val detailSql = "select "  + detailFilterColumn.split(",").map(col => "t2." + col).mkString(",") + " from " + detailTable + " as t2, " +
          "(select id, part, min(scantime) as scantime from " + detailTable + " where " + partDetailCondition + " group by id, part) as t1 " +
          "where t2.id=t1.id and t2.part = t1.part and t1.scantime=t2.scantime"
        var partDetailDf = IoUtils.getDfFromCockroachdb(spark, detailSql, numExecutors)


        val partList = partDetailDf.select("part").dropDuplicates().map(_.getString(0)).collect.toList
        val partCondition = "part in (" + partList.map(s => "'" + s + "'").mkString(",") + ")"
        println(partCondition)
//        partsn,vendor_code,date_code,part
        val partDetailColumn = partDetailColumnStr.split(",")

        partDetailDf = partDetailDf
          .selectExpr(partDetailColumn: _*)
          .withColumnRenamed("part", "component")

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
          .withColumn(componentInfo, transferArrayToString(col(componentInfo)))
          .drop("component")

        testDeailResultGroupByFirstDf.show(1, false)

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

        val datasetTableName = "`data_set_bigtable@" + id + "`"
        //drop dataset bigtable
        val dropSql = "DROP TABLE IF EXISTS " + datasetTableName
        mariadbUtils.execSqlToMariadb(dropSql)
        //truncate dataset bigtable schema
        val createSql = "CREATE TABLE " + datasetTableName + " (" + schema
        mariadbUtils.execSqlToMariadb(createSql)

        //insert 大表資料
        mariadbUtils.saveToMariadb(testDeailResultGroupByFirstDf, datasetTableName, numExecutors)
        //update dataset 設定的欄位
        val updateSql = "UPDATE data_set_setting" + " SET bt_name='" + datasetTableName.substring(1, datasetTableName.length - 1) + "'," +
          " bt_create_time = COALESCE(bt_create_time, '" + jobStartTime + "')," +
          " bt_last_time = '" + jobStartTime + "'," +
          " bt_next_time = '" + nextExcuteTime + "'" +
          " WHERE id = " + id
        mariadbUtils.execSqlToMariadb(updateSql)

        testDeailResultGroupByFirstDf

    }
}
