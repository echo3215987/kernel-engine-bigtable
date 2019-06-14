package com.foxconn.iisd.bd.test.rca

import java.io.FileNotFoundException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.test.config.ConfigLoaderTest
import com.foxconn.iisd.bd.test.rca.utils.IoUtilsTest
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable._

object KernelEngineTest{

    var configLoader = new ConfigLoaderTest("""conf/default.yaml""")
    val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

    def main(args: Array[String]): Unit = {

        val limit = 1
        var count = 0

        println("v1-test")

        while(count < limit) {
            println(s"count: $count")

            try {
                if(args.length == 1) {
                    configLoader = new ConfigLoaderTest(args(0))
                }
                KernelEngineTest.start()
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

        val flag = new java.util.Date().getTime().toString

        println(s"flag: $flag")

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val sparkConfigMap = configLoader.getString("minio", "bucket")

        val spark = SparkSession
            .builder
            .master(configLoader.getString("spark", "master"))
            .appName(configLoader.getString("spark", "job_name"))
            .getOrCreate()


        var logPathSection = "local_log_path"
        val isFromMinio = configLoader.getString("general", "from_minio").toBoolean

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

        val factory = configLoader.getString("general", "factory")
        val failCondition: Int = configLoader.getString("analysis", "fail_condition").toInt

        //s3a://" + bucket + "/
        val wipPath = configLoader.getString(logPathSection, "wip_path")
        val wipPartsPath = configLoader.getString(logPathSection, "wip_parts_path")
        val bobcatPath = configLoader.getString(logPathSection, "bobcat_path")

        val wipFileLmits = configLoader.getString(logPathSection, "wip_file_limits").toInt
        val wipPartsFileLmits = configLoader.getString(logPathSection, "wip_parts_file_limits").toInt
        val bobcatFileLmits = configLoader.getString(logPathSection, "bobcat_file_limits").toInt

        //"id,sn,wo,hh_part,cust_part,assembline,scantime,na1,na2,product,floor"
        //CN8BJ7C2G1_|_CN8BJ7C2G1_|_296200140_|_V1N03B#629_|_V1N03B#629_|_Main	B_|_2018-11-19	13:39:07.467_|__|__|_TaiJi	Base_|_D626
        val wipColumns = configLoader.getString("log_prop", "wip_col")

        //"id,partsn,scantime,opid,assembly_station,part,hh_pard,cust_part,line,disassembly"
        //CN8BJ7C2G1_|_C189220M8PFAC_|_2018-11-19 13:39:07.467_|_F1034891_|_Main B_|_LK_|_F5S43-60001_|_F5S43-60001_|_D626_|_0
        val wipPartsColumns = configLoader.getString("log_prop", "wip_parts_col")

        //"sn,station,stationcode,machine,start_time,end_time,istestfail,symptom,desc,uploadtime,emp,ver1,ver2"
        //CN8BS7C334_||_TLEOL_||_TLEOL_||_LC_TLEOL_41_||_11/29/2018 8:33:10 AM_||_11/29/2018 8:38:46 AM_||_0_||__||__||_11/29/2018 8:38:46 AM_||_ _||_ _||_
        val bobcatColumns = configLoader.getString("log_prop", "bobcat_col")

        val dataSeperator = configLoader.getString("log_prop", "log_seperator")

        //根據物料序號編碼規則解析VENDOR與DATECODE
        var vendorCodeMap = Map("" -> ("PARTSN", 0, 0))
        var dateCodeMap = Map("" -> ("PARTSN", 0, 0))

        val vendorCodeConfMap = configLoader.getSection("part_vendor_rules")

        vendorCodeConfMap.forEach((k: String, v: String) => {
            val vList = v.split(",")
            vendorCodeMap = vendorCodeMap + (k -> (vList(0), vList(1).toInt - 1, vList(2).toInt))
        })

        val dateCodeConfMap = configLoader.getSection("part_datecode_rules")

        dateCodeConfMap.forEach((k: String, v: String) => {
            val vList = v.split(",")
            dateCodeMap = dateCodeMap + (k -> (vList(0), vList(1).toInt - 1, vList(2).toInt))
        })

        vendorCodeMap.foreach(println)
        dateCodeMap.foreach(println)

        ///////////
        //載入資料//
        ///////////

        try {

//            val wipDestPath = IoUtilsTest.flatMinioFiles(spark,
//                flag,
//                wipPath,
//                wipFileLmits)
//
//            val wipSourceDf = IoUtilsTest.getDfFromPath(spark, wipDestPath.toString, wipColumns, dataSeperator)
//                .withColumn("product", regexp_replace($"product", "\t", " "))
//                .withColumn("scantime1", unix_timestamp(trim($"scantime"),
//                    configLoader.getString("log_prop", "wip_dt_fmt")).cast(TimestampType))
//                .drop("scantime")
//                .withColumnRenamed("scantime1", "scantime")
//                .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//            val wipPartsDestPath = IoUtilsTest.flatMinioFiles(spark,
//                flag,
//                wipPartsPath,
//                wipPartsFileLmits)
//
//            val wipPartsSourceDf = IoUtilsTest.getDfFromPath(spark, wipPartsDestPath.toString, wipPartsColumns, dataSeperator)
//                .withColumn("scantime1", unix_timestamp(trim($"scantime"),
//                    configLoader.getString("log_prop", "wip_parts_dt_fmt")).cast(TimestampType))
//                .drop("scantime")
//                .withColumnRenamed("scantime1", "scantime")
//
//            //需要改為查表之動態欄位
//            val vendorUdf = udf((partName: String, partSn: String) => {
//                if (vendorCodeMap.contains(partName)) {
//                    val source = vendorCodeMap(partName)
//
//                    if (partSn.length > source._3) {
//                        partSn.substring(source._2, source._3)
//                    }
//                    else {
//                        partSn
//                    }
//                }
//                else
//                    partSn
//            })
//
//            val dateUdf = udf((partName: String, partSn: String) => {
//                if (dateCodeMap.contains(partName)) {
//                    val source = dateCodeMap(partName)
//
//                    if (partSn.length > source._3) {
//                        partSn.substring(source._2, source._3)
//                    } else {
//                        partSn
//                    }
//                }
//                else
//                    partSn
//            })
//
//            val wipPartsDetailSourceDf = wipPartsSourceDf
//                .withColumn("vendor_code", vendorUdf($"part", $"partsn"))
//                .withColumn("date_code", dateUdf($"part", $"partsn"))
//                .withColumn("vendor_date_code", concat($"vendor_code", lit("_"), $"date_code"))
//                .persist(StorageLevel.MEMORY_AND_DISK_SER_2)
//
//            import org.apache.spark.sql.functions.udf
//
//            val time_group_duration_min = 10
//
//            //performance issue of DateTimeFormatter
//            val startTimeGroupUdf = udf((startTimeStr: String)
//            => {
//                val bobcatDatetimeFormatter = DateTimeFormatter
//                    .ofPattern(configLoader.getString("log_prop", "bobcat_dt_fmt"), Locale.US)
//
//                val startTime = LocalDateTime
//                    .parse(startTimeStr, bobcatDatetimeFormatter)
//
//                startTime
//                    .minusMinutes(startTime.getMinute % time_group_duration_min)
//                    .withSecond(0)
//                    .format(bobcatDatetimeFormatter)
//            }
//            )
//
//            val endTimeGroupUdf = udf((startTimeStr: String)
//            => {
//                val bobcatDatetimeFormatter = DateTimeFormatter
//                    .ofPattern(configLoader.getString("log_prop", "bobcat_dt_fmt"), Locale.US)
//
//                val startTime = LocalDateTime
//                    .parse(startTimeStr, bobcatDatetimeFormatter)
//
//                startTime
//                    .plusMinutes(time_group_duration_min - (startTime.getMinute % time_group_duration_min))
//                    .withSecond(0)
//                    .format(bobcatDatetimeFormatter)
//            }
//            )
//
//            val symptomArrayUdf = udf((symptom: String)
//            => {
//                symptom
//                    .split(";")
//                    .map(_.trim)
//            }
//            )
//
//            val machineUdf = udf((station: String, stationCode: String, machine: String) => {
//                if (machine.isEmpty) {
//                    if (stationCode.isEmpty) {
//                        station
//                    } else {
//                        stationCode
//                    }
//                } else {
//                    machine
//                }
//
//            })
//
//            val bobcatDestPaths = IoUtilsTest.flatMinioFiles(spark,
//                flag,
//                bobcatPath,
//                bobcatFileLmits)
//
//            val bobcatSourceDf = IoUtilsTest.getDfFromPath(spark, bobcatDestPaths.toString, bobcatColumns, dataSeperator)
//                .withColumn("factory", lit(factory))
//                .withColumn("start_time_new", when(trim($"start_time") === "", $"end_time").otherwise($"start_time"))
//                .drop("start_time")
//                .withColumnRenamed("start_time_new", "start_time")
//                .withColumn("ori_symptom", $"symptom")
//                .withColumn("start_time1",
//                    unix_timestamp(trim($"start_time"),
//                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
//                .withColumn("start_time_group",
//                    unix_timestamp(startTimeGroupUdf(trim($"start_time")),
//                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
//                .withColumn("end_time_group",
//                    unix_timestamp(endTimeGroupUdf(trim($"start_time")),
//                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
//                .withColumn("end_time1",
//                    unix_timestamp(trim($"end_time"),
//                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
//                .withColumn("uploadtime1",
//                    unix_timestamp(trim($"uploadtime"),
//                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
//                .withColumn("machine_1", machineUdf($"station", $"stationcode", $"machine"))
//                .drop("start_time")
//                .drop("end_time")
//                .drop("uploadtime")
//                .drop("machine")
//                .withColumnRenamed("start_time1", "start_time")
//                .withColumnRenamed("end_time1", "end_time")
//                .withColumnRenamed("uploadtime1", "uploadtime")
//                .withColumnRenamed("machine_1", "machine")
//                .persist(StorageLevel.MEMORY_AND_DISK_SER)
//
//            println(wipSourceDf.count() + ", " + wipSourceDf.distinct().count())
//            wipSourceDf.show()
//
//            println(wipPartsDetailSourceDf.count() + ", " + wipPartsDetailSourceDf.distinct().count())
//            wipPartsDetailSourceDf.show()
//
//            println(bobcatSourceDf.count() + ", " + bobcatSourceDf.distinct().count())
//            bobcatSourceDf.show()
//
//            //將資料儲存進Cockroachdb
//            println("saveToCockroachdb --> wipSourceDf")
//            IoUtilsTest.saveToCockroachdb(wipSourceDf.withColumn("factory", lit(factory)),
//                configLoader.getString("log_prop", "wip_table"),
//                numExecutors)
//
//            println("saveToCockroachdb --> wipPartsDetailSourceDf")
//            IoUtilsTest.saveToCockroachdb(wipPartsDetailSourceDf.withColumn("factory", lit(factory)),
//                configLoader.getString("log_prop", "wip_parts_table"),
//                numExecutors)
//
//            println("saveToCockroachdb --> bobcatSourceDf")
//            IoUtilsTest.saveToCockroachdb(bobcatSourceDf,
//                configLoader.getString("log_prop", "bobcat_table"),
//                numExecutors)

//            ////////////////////////////
//            //抓取和該次測試有關的組裝資料//
//            ////////////////////////////
//            val bobcatSnArr = bobcatSourceDf
//                .select("sn")
//                .distinct()
//                .rdd
//                .map(r => r(0).asInstanceOf[String])
//                .collect

            val bobcatSnArr = Array("CN95A8C1Q7")

            println("bobcatSnArr")
            println(bobcatSnArr.mkString(","))

            val bobcatTmpDbPredicates = Array[String]("sn in (" + bobcatSnArr.map(s => "'" + s + "'").mkString(",") + ") and ori_symptom = symptom")

            println(bobcatTmpDbPredicates.mkString("&&"))

            val bobcatTmpDbDf = IoUtilsTest
                .getDfFromCockroachdb(spark, configLoader.getString("log_prop", "bobcat_table"), bobcatTmpDbPredicates)
                .select(bobcatColumns.split(",").map(name => col(name)): _*)
                .distinct()
                .orderBy("start_time")

            println("bobcatTmpDbDf")
            bobcatTmpDbDf.show()

            //計算truefail
            val failCntInitSpec = Window
                .partitionBy("sn", "station")
                .orderBy("sn", "start_time", "symptom", "desc_", "stationcode", "machine", "start_time", "end_time", "uploadtime")

            //by sn, station 累計 fail_cnt & 確認true fail ＆ fail symptom
            val bobcatDbAggTmpDf_1 = bobcatTmpDbDf
                .withColumn("fail_cnt_1", sum(bobcatTmpDbDf("istestfail")).over(failCntInitSpec).cast(IntegerType))
                .drop("fail_cnt")
                .withColumnRenamed("fail_cnt_1", "fail_cnt")
                .withColumn("is_true_fail_1", when($"fail_cnt" >= failCondition, 1).otherwise(0))
                .withColumn("is_symptom_fail_1", when($"is_true_fail_1" === 1 and $"symptom" =!= "", 1).otherwise(0))
                .drop("is_true_fail")
                .drop("is_symptom_fail")
                .withColumnRenamed("is_true_fail_1", "is_true_fail")
                .withColumnRenamed("is_symptom_fail_1", "is_symptom_fail")

            println("bobcatDbAggTmpDf_1")
            bobcatDbAggTmpDf_1.show()

            val failCntSpec = Window
                .partitionBy("sn")
                .orderBy("sn", "start_time", "symptom", "desc_", "stationcode", "machine", "start_time", "end_time", "uploadtime")

            //by sn 重新累計 fail_cnt
            val bobcatDbAggTmpDf_2 = bobcatDbAggTmpDf_1
                .withColumn("fail_cnt_1", sum(bobcatDbAggTmpDf_1("is_true_fail")).over(failCntSpec).cast(IntegerType))
                .withColumn("is_true_fail_1", when($"fail_cnt_1" >= 1, 1).otherwise(0))

            println("bobcatDbAggTmpDf_2")
            bobcatDbAggTmpDf_2.show()

            //by sn 重新累計 fail_cnt，並將fail_cnt數值由第一次true fail後開始累加，並將symptoms展開
            val bobcatDbAggDf = bobcatDbAggTmpDf_2
                .withColumn("fail_cnt_2", sum(bobcatDbAggTmpDf_2("is_true_fail_1")).over(failCntSpec).cast(IntegerType))
                .drop("fail_cnt")
                .drop("fail_cnt_1")
                .drop("is_true_fail_1")
                .withColumnRenamed("fail_cnt_2", "fail_cnt")
                .withColumn("symptom_1", explode(split($"symptom", ";")))
                .withColumn("symptom_2", trim($"symptom_1"))
                .drop("symptom_1")
                .drop("symptom")
                .withColumnRenamed("symptom_2", "symptom")
                .persist(StorageLevel.MEMORY_AND_DISK_SER)

            println("bobcatDbAggDf")
            bobcatDbAggDf.show()
//
//            println("saveToCockroachdb --> bobcatDbAggDf")
//            IoUtilsTest.saveToCockroachdb(bobcatDbAggDf,
//                configLoader.getString("log_prop", "bobcat_table"),
//                numExecutors)
//
//            val wipDbPredicates = Array[String]("sn in (" + bobcatSnArr.map(s => "'" + s + "'").mkString(",") + ")")
////            println(wipDbPredicates.mkString(","))
//
//            val wipDbDf = IoUtilsTest
//                .getDfFromCockroachdb(spark, configLoader.getString("log_prop", "wip_table"), wipDbPredicates)
//                //.where($"sn".isin(bobcatSnArr: _*))
//                .withColumnRenamed("scantime", "wip_scantime")
//                .persist(StorageLevel.MEMORY_AND_DISK_SER)
//
////            println("wipDbDf")
////            wipDbDf.show()
//
//            val wipPartsIdArr = wipDbDf
//                .select("id")
//                .distinct()
//                .rdd
//                .map(r => r(0).asInstanceOf[String])
//                .collect()
//
////            println("wipPartsIdArr")
////            println(wipPartsIdArr.mkString(","))
//
//            val wipPartsDetailDbPredicates = Array[String]("id in (" + wipPartsIdArr.map(s => "'" + s + "'").mkString(",") + ")")
////            println(wipPartsDetailDbPredicates.mkString(","))
//
//            val wipPartsDetailDbDf = IoUtilsTest
//                .getDfFromCockroachdb(spark, configLoader.getString("log_prop", "wip_parts_table"), wipPartsDetailDbPredicates)
//                //.where($"id".isin(wipPartsIdArr: _*))
//                .persist(StorageLevel.MEMORY_AND_DISK_SER)
//
////            println("wipPartsDetailDbDf")
////            wipPartsDetailDbDf.show()
//
//            ////////////////////
//            //整合工單及組裝資訊//
//            ////////////////////
//            val lineUdf = udf((floor: String, line: String) => {
//                if (line.isEmpty) {
//                    floor
//                } else {
//                    line
//                }
//
//            })
//
//            val wipPartFullDbDf = wipPartsDetailDbDf
//                .join(wipDbDf, Seq("id", "factory"))
////                .where($"wip_scantime" <= $"scantime")
////                .orderBy("wip_scantime", "scantime")
//                .withColumn("line_1", lineUdf($"floor", $"line"))
//                .drop("line")
//                .withColumnRenamed("line_1", "line")
//                .persist(StorageLevel.MEMORY_AND_DISK_SER)
//
////            println("wipPartFullDbDf")
////            wipPartFullDbDf.show()
//
//            ///////////////////
//            //分析測試、組裝資料//
//            ///////////////////
//
//            val mariadbUtils = new MariadbUtilsTest()
//
//            bobcatSourceDf
//                .select("start_time_group", "end_time_group")
//                .distinct()
//                .orderBy("start_time_group")
//                .rdd
//                .map(r => (r(0).asInstanceOf[java.sql.Timestamp], r(1).asInstanceOf[java.sql.Timestamp]))
//                .collect()
//                .foreach(timeTuple => {
//
//                    val start_time_group = timeTuple._1
//                    val end_time_group = timeTuple._2
//
//                    val bobcatSnPartialArr = bobcatSourceDf
//                        .where($"start_time_group" === start_time_group)
//                        .select("sn")
//                        .distinct()
//                        .rdd
//                        .map(r => r(0).asInstanceOf[String])
//                        .collect
//
//                    val bobcatPartialDf = bobcatDbAggDf
//                        .where((($"istestfail" === 0 and $"fail_cnt" === 0) or ($"istestfail" === 1 and $"fail_cnt" === 1)) and $"start_time_group" === start_time_group)
//                        .orderBy("sn", "start_time")
//
//
//                    if(configLoader.getString("mariadb", "database").equals("rca-ipbd-lx-monitor")) {
//
//                        println("rca-ipbd-lx-monitor join logical")
//
//                        val wipPartFullPartialDf = wipPartFullDbDf
//                            .where($"scantime" <= end_time_group and $"sn".isin(bobcatSnPartialArr: _*))
//                            .orderBy("sn", "wip_scantime", "scantime")
//
//                        println(start_time_group)
//                        //                    println("bobcatSnPartialArr")
//                        //                    println(bobcatSnPartialArr.mkString(","))
//                        println("bobcatPartialDf")
//                        bobcatPartialDf.show()
//                        println("wipPartFullPartialDf")
//                        wipPartFullPartialDf.show()
//
//                        val analyzer = new AnalyzerTest()
//
//                        analyzer.analysis(
//                            spark,
//                            mariadbUtils,
//                            start_time_group.toLocalDateTime,
//                            failCondition,
//                            wipPartFullPartialDf,
//                            bobcatPartialDf,
//                            numExecutors)
//                    } else {
//                        val wipPartFullPartialDf = wipPartFullDbDf
//                            .where($"scantime" <= end_time_group and $"wip_scantime" <= $"scantime" and $"sn".isin(bobcatSnPartialArr: _*))
//                            .orderBy("sn", "wip_scantime", "scantime")
//
//                        println(start_time_group)
//                        //                    println("bobcatSnPartialArr")
//                        //                    println(bobcatSnPartialArr.mkString(","))
//                        println("bobcatPartialDf")
//                        bobcatPartialDf.show()
//                        println("wipPartFullPartialDf")
//                        wipPartFullPartialDf.show()
//
//                        val analyzer = new AnalyzerTest()
//
//                        analyzer.analysis(
//                            spark,
//                            mariadbUtils,
//                            start_time_group.toLocalDateTime,
//                            failCondition,
//                            wipPartFullPartialDf,
//                            bobcatPartialDf,
//                            numExecutors)
//                    }
//
//                })
//
//            ///////////////////////////////
//            //寫入新增product, floor, line//
//            ///////////////////////////////
//            val productFloorLineDbDf = mariadbUtils
//                .getDfFromMariadb(spark, "product_floor_line")
//                .drop("update_time")
//                .select("product", "floor", "line")
//
//            val productFloorLineFileDf = wipPartFullDbDf
//                .select("product", "floor", "line")
//                .distinct()
//
//            val productFloorLineDiffDf = productFloorLineFileDf
//                .except(productFloorLineDbDf)
//
//            productFloorLineDbDf.show()
//            productFloorLineFileDf.show()
//            productFloorLineDiffDf.show()
//
//            mariadbUtils.saveToMariadb(
//                productFloorLineDiffDf
//                    .select("product", "floor", "line")
//                    .distinct(),
//                "product_floor_line",
//                numExecutors)
        } catch {
            case ex: FileNotFoundException => {
                //                ex.printStackTrace()
                println("===> FileNotFoundException !!!")
            }
        }
    }
}
