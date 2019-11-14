package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.foxconn.iisd.bd.rca.SparkUDF.{genInfo, genItemJsonFormat, genStaionJsonFormat, genTestDetailSelectSql, getFirstOrLastRow, transferArrayToString}
import com.foxconn.iisd.bd.rca.XWJBigtable.{configContext, configLoader}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.Seq

object Bigtable{

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


  def createBigtable(configContext: ConfigContext, row: Row, currentDatasetDf: DataFrame,
                       currentDatasetStationItemDf: DataFrame, id: String,
                       jobStartTime: String, nextExcuteTime: String) ={
        val sparkSession = configContext.sparkSession
        import sparkSession.implicits._

        //只撈出該資料集distinct的item, 只有選的station與item
        val currentDatasetStationItemDF = currentDatasetStationItemDf.withColumn("selectSql",
            genTestDetailSelectSql(array(lit("test_value"), lit("test_item_result"), lit("test_item_result_detail")),
                col("item")))

        var testDeailResultGroupByFirstDf = sparkSession.emptyDataFrame

        val aggFunction = List("min", "max")
        val aggMap = Map("min" -> "first", "max" -> "last")

        val currentDatasetStationItemList = currentDatasetStationItemDF.select("station_name", "selectSql").collect.toList

        println("-----------------> select testdetail first / last -> start_time:"
          + new SimpleDateFormat(configContext.jobDateFmt).format(new Date().getTime()))

        for(stationItemRow <- currentDatasetStationItemList){
            //撈測試結果細表的條件
            //子查詢改用過濾初步條件後, 撈回來的資料用spark group by取第一筆在memory處理k
              val whereSql = Utils.genTestDetailWhereSQL(row.getAs("product"), stationItemRow.getAs("station_name")) + " and id is not null"
              val testdetailSql = "select " + configContext.testDetailFilterColumns.split(",").mkString(",") + "," +
                        stationItemRow.getAs("selectSql") + " from " + configContext.cockroachDbTestDetailTable + whereSql
              val tempDf = configContext.cockroachDBIo.getDfFromCockroachdb(sparkSession, testdetailSql, configContext.sparkNumExcutors)
              if (testDeailResultGroupByFirstDf.isEmpty)
                  testDeailResultGroupByFirstDf = tempDf
              else
                  testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.union(tempDf)

        }
        // 取scantime第一筆或最後一筆
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumn("asc_rank", rank().over(wSpecTestDetailAsc))
          .withColumn("desc_rank", rank().over(wSpecTestDetailDesc))
          .where($"asc_rank".equalTo(1).or($"desc_rank".equalTo(1)))
          .withColumn("value_rank", getFirstOrLastRow($"asc_rank", $"desc_rank"))
          .withColumn("value_rank", explode($"value_rank")).persist(StorageLevel.MEMORY_AND_DISK_SER)

        println("-----------------> select testdetail first / last -> end_time:"
          + new SimpleDateFormat(configContext.jobDateFmt).format(new Date().getTime()))

        val fieldsColumnDataType = testDeailResultGroupByFirstDf.schema.fields
  //      Cartridge大產品(id 改成 sn)
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn("sn", col("id"))

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
        configContext.stationInfoColumns.split(",").foreach(attr => {
            val attrStr = attr + "_str"
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
              .withColumn(attrStr, genStaionJsonFormat(col(configContext.stationName), lit(attr), col(attr)))
            map = map + (attrStr -> "collect_set")
            list = list :+ "collect_set(" + attrStr + ")"
        })

        var itemInfoList = List[String]()
        //以每個dataset, 收斂成一個測項資訊
        configContext.itemInfoColumns.split(",").foreach(attr => {
            val attrStr = attr + "_str"
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.withColumn(attrStr,
                genItemJsonFormat(col(configContext.stationName), col(configContext.item), col(attr), lit(attr)))
            map = map + (attrStr -> "collect_set")
            itemInfoList = itemInfoList :+ "collect_set(" + attrStr + ")"
        })

        //group by 並收斂工站與測項資訊
        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf.groupBy("id", "sn", "value_rank")
          .agg(map)
          .withColumn(configContext.stationInfo, array())
          .withColumn(configContext.itemInfo, array())

        list.foreach(ele => {
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
              .withColumn(configContext.stationInfo, genInfo(col(configContext.stationInfo), col(ele)))
              .drop(ele) //刪除collect_set的多個工站資訊欄位
        })

        itemInfoList.foreach(ele => {
            testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
              .withColumn(configContext.itemInfo,
                  genInfo(col(configContext.itemInfo), col(ele)))
              .drop(ele) //刪除collect_set的多個工站資訊欄位
        })

        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumn(configContext.stationInfo, transferArrayToString(col(configContext.stationInfo)))
          .withColumn(configContext.itemInfo, transferArrayToString(col(configContext.itemInfo)))
          .join(currentDatasetDf.select("id", "name", "product", "component"), Seq("id"), "left")
          //新增sn_type, 紀錄是整機還是非整機測試(比對測試明細sn vs unit_number), 目前先維持定值, 之後有需要分整機或非整機測試再處理
          .withColumn("sn_type", lit("wip")) //unit

        val woList = partMasterDf
            .filter(col("wo").notEqual("N/A"))
            .select("wo").dropDuplicates().map(_.getString(0)).collect.toList
        var woDf = sparkSession.emptyDataFrame
        var comDf = sparkSession.emptyDataFrame
        var partDetailDf = sparkSession.emptyDataFrame
        if(woList.size > 0){

          val woCondition = "wo in (" + woList.map(s => "'" + s + "'").mkString(",") + ")"
          //        println(woCondition)
          val woWhereSql = "select wo,wo_type,plant_code,plan_qty,config,build_name,release_date from " +
            configContext.cockroachDbWoTable + " where " + woCondition
          woDf = configContext.cockroachDBIo.getDfFromCockroachdb(sparkSession, woWhereSql, configContext.sparkNumExcutors)

          //只撈關鍵物料的component
          val configList = woDf.select("config").dropDuplicates().map(_.getString(0)).collect.toList
          val componentList = currentDatasetDf.selectExpr("explode(component)").dropDuplicates().map(_.getString(0)).collect.toList

          if(configList.size > 0 && componentList.size > 0){
            val componentCondition = "config in (" + configList.map(s => "'" + s + "'").mkString(",") + ") " +
              "and component in (" + componentList.map(s => "'" + s + "'").mkString(",") + ")"

            val comWhereSql = "select config,vendor,hhpn,oempn,component,component_type,input_qty from " +
              configContext.cockroachDbMatTable + " where " + componentCondition
            comDf = configContext.cockroachDBIo.getDfFromCockroachdb(sparkSession, comWhereSql, configContext.sparkNumExcutors)
          }

          val partMasterIdList = partMasterDf.select("id").dropDuplicates().map(_.getString(0)).collect.toList

          if(partMasterIdList.size>0 && componentList.size>0){
            //dataset選的關鍵物料
            val partDetailCondition = "id in (" + partMasterIdList.map(s => "'" + s + "'").mkString(",") + ") " +
              " and part in (" + componentList.map(s => "'" + s + "'").mkString(",") + ")"

            val partDetailWhereSql = "select " + configContext.detailFilterColumns.split(",").mkString(",") +
              " from " + configContext.cockroachDbDetailTable + " where " + partDetailCondition
            partDetailDf = configContext.cockroachDBIo.getDfFromCockroachdb(sparkSession, partDetailWhereSql, configContext.sparkNumExcutors)

            //取第一筆scantime組裝細表資料
            //partsn,vendor_code,date_code,part
            val partDetailColumn =  configContext.detailColumns.split(",")
            partDetailDf = partDetailDf
              .withColumn("rank", rank().over(wSpecDetailAsc))
              .where($"rank".equalTo(1))
              .selectExpr(partDetailColumn: _*)
              .withColumnRenamed("part", "component")
          }

        }

        partMasterDf = partMasterDf
          .select("sn", "floor", "scantime", "wo", "line")
          .dropDuplicates()

        if(comDf.isEmpty){
          testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
            .join(partMasterDf, Seq("sn"), "left")
            .withColumn(configContext.componentInfo, lit(null).cast(StringType))
            .drop("component")

        }else{
          comDf = comDf.join(partDetailDf, Seq("component"), "left")

          //以每個dataset, 收斂成一個關鍵物料資訊
          var comConfigMap = Map[String, String]()
          var comConfigList = List[String]()
          configContext.componentInfoColumns.split(",").foreach(attr => {
            val attrStr = attr + "_str"
            comDf = comDf
              .withColumn(attrStr, genStaionJsonFormat(col(configContext.component), lit(attr), col(attr)))
            comConfigMap = comConfigMap + (attrStr -> "collect_set")
            comConfigList = comConfigList :+ "collect_set(" + attrStr + ")"
          })

          var datasetComponentDF = testDeailResultGroupByFirstDf.select("component").dropDuplicates()
            .withColumn("component", explode(col("component")))
            .join(comDf, Seq("component"), "left")

          //group by 並收斂關鍵物料資訊
          datasetComponentDF = datasetComponentDF.groupBy("config")
            .agg(comConfigMap)
            .withColumn(configContext.componentInfo, array())

          comConfigList.foreach(ele => {
            datasetComponentDF = datasetComponentDF
              .withColumn(configContext.componentInfo, genInfo(col(configContext.componentInfo), col(ele)))
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
            .withColumn(configContext.componentInfo, transferArrayToString(col(configContext.componentInfo)))
            .drop("component")
        }

        //create dataset bigtable schema
        val schema =
          "data_set_name varchar(200) Not NULL," +
          "data_set_id varchar(30) Not NULL," +
          "product varchar(50) Not NULL," +
          "sn varchar(100) Not NULL," +
          "sn_type varchar(100) DEFAULT NULL," +
          "value_rank varchar(30) Not NULL," +
          "station_info json Not NULL," +
          "item_info json Not NULL," +
          "floor varchar(50) DEFAULT NULL," + //組裝樓層
          "line varchar(50) DEFAULT NULL," + //組裝線別
          "scantime datetime DEFAULT NULL," +
          "wo varchar(50) DEFAULT NULL," +
          "wo_type varchar(50) DEFAULT NULL," +
          "plant_code varchar(50) DEFAULT NULL," +
          "plan_qty varchar(50) DEFAULT NULL," +
          "config varchar(50) DEFAULT NULL," +
          "build_name varchar(50) DEFAULT NULL," +
          "component_info json DEFAULT NULL," +
          "PRIMARY KEY (data_set_id,product,sn,value_rank)" +
          ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"

        testDeailResultGroupByFirstDf = testDeailResultGroupByFirstDf
          .withColumnRenamed("name", "data_set_name")
          .withColumnRenamed("id", "data_set_id")
  //          .persist(StorageLevel.MEMORY_AND_DISK_SER)
          .persist(StorageLevel.DISK_ONLY)

        val datasetTableName = "`data_set_bigtable@" + id + "`"
        //drop dataset bigtable
        val dropSql = "DROP TABLE IF EXISTS " + datasetTableName
        configContext.mysqlDBIo.execSqlToMariadb(dropSql)

        //truncate dataset bigtable schema
        val createSql = "CREATE TABLE " + datasetTableName + " (" + schema
        configContext.mysqlDBIo.execSqlToMariadb(createSql)

        //insert 大表資料  -> 改成 df.write
        println("-----------------> insert data: " + id + ", start_time:" + new SimpleDateFormat(
          configContext.jobDateFmt).format(new Date().getTime()))

          configContext.mysqlDBIo.saveToMariadb(testDeailResultGroupByFirstDf, datasetTableName, SaveMode.Append, configContext.numPartitions)

        println("-----------------> insert data: " + id + ", end_time:" + new SimpleDateFormat(
          configContext.jobDateFmt).format(new Date().getTime()))

        (testDeailResultGroupByFirstDf, fieldsColumnDataType)
    }
}
