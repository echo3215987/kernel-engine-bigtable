package com.foxconn.iisd.bd.test.rca

import java.time.LocalDateTime

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import com.foxconn.iisd.bd.test.rca.KernelEngineTest.datetimeFormatter

class AnalyzerTest {

    ////////
    //分析//
    ////////
    def analysis(spark: SparkSession, mariadbUtils: MariadbUtilsTest, start_time_group: LocalDateTime, failCondition: Int,
                 wipPartFullSourceDf: DataFrame, bobcatDf: DataFrame): Unit = { //Map[String, DataFrame] = {

        import spark.implicits._

        val start_time_group_str = start_time_group.format(datetimeFormatter)

        val fullyJoinDf = bobcatDf
          .join(wipPartFullSourceDf, Seq("sn", "factory"))
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

        //risk_test_station_sn
        val riskTestStationSnDf = fullyJoinDf
          .where($"wip_scantime" <=  $"start_time")
          .select($"sn", $"start_time_group", $"product", $"factory", $"floor", $"line", $"station".alias("_station"), $"machine", $"symptom", $"station", $"start_time", $"istestfail", $"is_true_fail")
          .distinct()
          .withColumnRenamed("start_time_group", "test_starttime")
          .withColumnRenamed("_station", "riskname")
          .withColumnRenamed("machine", "riskcode")
          .withColumnRenamed("symptom", "failure_sympton")

        riskTestStationSnDf.show(false)

//        mariadbUtils.execSqlToMariadb(s"delete from risk_test_station_sn where test_starttime = '$start_time_group_str'")
//        mariadbUtils.saveToMariadb(riskTestStationSnDf, "risk_test_station_sn")

        //risk_assembly_station_sn
        val riskAssemblyStationSnDf = fullyJoinDf
          .where($"scantime" <=  $"start_time")
          .select("sn", "start_time_group", "product", "factory", "floor", "line", "assembly_station", "opid", "symptom", "station", "start_time", "istestfail", "is_true_fail")
          .distinct()
          .withColumnRenamed("start_time_group", "test_starttime")
          .withColumnRenamed("assembly_station", "riskname")
          .withColumnRenamed("opid", "riskcode")
          .withColumnRenamed("symptom", "failure_sympton")

        riskAssemblyStationSnDf.show(false)

//        mariadbUtils.execSqlToMariadb(s"delete from risk_assembly_station_sn where test_starttime = '$start_time_group_str'")
//        mariadbUtils.saveToMariadb(riskAssemblyStationSnDf, "risk_assembly_station_sn")

        //risk_part_sn
        val riskPartSnDf = fullyJoinDf
          .where($"scantime" <= $"start_time")
          .select("sn", "start_time_group", "product", "factory", "floor", "line", "part", "vendor_date_code", "symptom", "station", "start_time", "istestfail", "is_true_fail")
          .distinct()
          .withColumnRenamed("start_time_group", "test_starttime")
          .withColumnRenamed("part", "riskname")
          .withColumnRenamed("vendor_date_code", "riskcode")
          .withColumnRenamed("symptom", "failure_sympton")

        riskPartSnDf.show(false)

//        mariadbUtils.execSqlToMariadb(s"delete from risk_part_sn where test_starttime = '$start_time_group_str'")
//        mariadbUtils.saveToMariadb(riskPartSnDf, "risk_part_sn")

        fullyJoinDf.unpersist()
    }
}
