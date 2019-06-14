package com.foxconn.iisd.bd.rca

import java.time.LocalDateTime

import com.foxconn.iisd.bd.rca.XWJKernelEngine.datetimeFormatter
import com.foxconn.iisd.bd.rca.utils.Summary
import com.foxconn.iisd.bd.rca.utils.db.MariadbUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

class Analyzer {

    ////////
    //分析//
    ////////
    def analysis(spark: SparkSession, mariadbUtils: MariadbUtils, start_time_group: LocalDateTime, failCondition: Int,
                 wipPartFullSourceDf: DataFrame, bobcatDf: DataFrame, numExecutors: Int, flag: String): Long = { //Map[String, DataFrame] = {

        import spark.implicits._

        val start_time_group_str = start_time_group.format(datetimeFormatter)

        val fullyJoinDf = bobcatDf
            .join(wipPartFullSourceDf, Seq("sn", "factory"))
            .where($"scantime" <=  $"start_time")
            .persist(StorageLevel.MEMORY_AND_DISK_SER)

        //risk_test_station_sn
        val riskTestStationSnDf = fullyJoinDf
            .select($"sn", $"start_time_group", $"product", $"factory", $"floor", $"line", $"station".alias("_station"),
                $"machine", $"symptom", $"station", $"start_time", $"istestfail", $"is_true_fail")
            .distinct()
            .withColumnRenamed("start_time_group", "test_starttime")
            .withColumnRenamed("_station", "riskname")
            .withColumnRenamed("machine", "riskcode")
            .withColumnRenamed("symptom", "failure_sympton")

        mariadbUtils.saveToMariadb(riskTestStationSnDf, "risk_test_station_sn", numExecutors)

        //risk_assembly_station_sn
        val riskAssemblyStationSnDf = fullyJoinDf
            .select($"sn", $"start_time_group", $"product", $"factory", $"floor", $"line", $"assembly_station", upper($"opid").alias("opid"),
                $"symptom", $"station", $"start_time", $"istestfail", $"is_true_fail")
            .distinct()
            .withColumnRenamed("start_time_group", "test_starttime")
            .withColumnRenamed("assembly_station", "riskname")
            .withColumnRenamed("opid", "riskcode")
            .withColumnRenamed("symptom", "failure_sympton")

        mariadbUtils.saveToMariadb(riskAssemblyStationSnDf, "risk_assembly_station_sn", numExecutors)

        //risk_part_sn
        val riskPartSnDf = fullyJoinDf
            .select("sn", "start_time_group", "product", "factory", "floor", "line", "part", "vendor_date_code",
                "symptom", "station", "start_time", "istestfail", "is_true_fail")
            .distinct()
            .withColumnRenamed("start_time_group", "test_starttime")
            .withColumnRenamed("part", "riskname")
            .withColumnRenamed("vendor_date_code", "riskcode")
            .withColumnRenamed("symptom", "failure_sympton")

        mariadbUtils.saveToMariadb(riskPartSnDf, "risk_part_sn", numExecutors)

        if(flag.equals("first")) {
            Summary.setMasterFirstRowTimeInMysqldb(getFirstStartTimeStr(spark, riskAssemblyStationSnDf))
            Summary.setDetailFirstRowTimeInMysqldb(getFirstStartTimeStr(spark, riskPartSnDf))
            Summary.setTestFirstRowTimeInMysqldb(getFirstStartTimeStr(spark, riskTestStationSnDf))
        } else if(flag.equals("last")) {
            Summary.setMasterLastRowTimeInMysqldb(getLastStartTimeStr(spark, riskAssemblyStationSnDf))
            Summary.setDetailLastRowTimeInMysqldb(getLastStartTimeStr(spark, riskPartSnDf))
            Summary.setTestLastRowTimeInMysqldb(getLastStartTimeStr(spark, riskTestStationSnDf))
        } else {

        }

        fullyJoinDf.unpersist()

        println("riskTestStationSnDf.count() : " + riskTestStationSnDf.count())
        val riskTestStationSnDfCount = Summary.getTestTotalRowsInMysqldb() + riskTestStationSnDf.count()
        println("riskTestStationSnDfCount : " + riskTestStationSnDfCount)
        Summary.setTestTotalRowsInMysqldb(riskTestStationSnDfCount)

        println("riskAssemblyStationSnDf.count() : " + riskAssemblyStationSnDf.count())
        val riskAssemblyStationSnDfCount = Summary.getMasterTotalRowsInMysqldb() + riskAssemblyStationSnDf.count()
        println("riskAssemblyStationSnDfCount : " + riskAssemblyStationSnDfCount)
        Summary.setMasterTotalRowsInMysqldb(riskAssemblyStationSnDfCount)

        println("riskPartSnDf.count() : " + riskPartSnDf.count())
        val riskPartSnDfCount = Summary.getDetailTotalRowsInMysqldb() + riskPartSnDf.count()
        println("riskPartSnDfCount : " + riskPartSnDfCount)
        Summary.setDetailTotalRowsInMysqldb(riskPartSnDfCount)

        riskTestStationSnDf.count() + riskAssemblyStationSnDf.count() + riskPartSnDf.count()
    }

    def getFirstStartTimeStr (spark: SparkSession, df: DataFrame): String = {
        import spark.implicits._

        val dfFirstStartTimeList = df
          .select("start_time")
          .limit(1)
          .withColumn("start_time2", $"start_time".cast(StringType))
          .select("start_time2")
          .rdd
          .map(r => r(0))
          .collect
          .toList
          .asInstanceOf[List[String]]
//        println("===> dfFirstStartTimeList(0) : " + dfFirstStartTimeList(0))
        dfFirstStartTimeList(0)
    }

    def getLastStartTimeStr (spark: SparkSession, df: DataFrame): String = {
        import spark.implicits._
        val dfLastTimeList = df
          .select("start_time")
          .orderBy($"start_time".desc)
          .limit(1)
          .withColumn("start_time2", $"start_time".cast(StringType))
          .select("start_time2")
          .rdd
          .map(r => r(0))
          .collect
          .toList
          .asInstanceOf[List[String]]
//        println("===> dfLastTimeList(0) : " + dfLastTimeList(0))
        dfLastTimeList(0)
    }
}
