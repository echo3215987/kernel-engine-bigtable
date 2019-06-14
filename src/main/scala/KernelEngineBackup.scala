import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.config.ConfigLoader
import com.foxconn.iisd.bd.rca.utils.{IoUtils, Summary}
import com.foxconn.iisd.bd.rca.utils.db._
import com.foxconn.iisd.bd.rca.Analyzer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable._

object KernelEngineBackup{

    var configLoader = new ConfigLoader()
    val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

    def main(args: Array[String]): Unit = {

        val limit = 1
        var count = 0

        println("v2")

        while(count < limit) {
            println(s"count: $count")

            try {
                configLoader.setDefaultConfigPath("""conf/default.yaml""")
                if(args.length == 1) {
                    configLoader.setDefaultConfigPath(args(0))
                }
                //KernelEngineBackup.start()
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

        var date: java.util.Date = new java.util.Date();
        val flag = date.getTime().toString
        val jobStartTime: String = new SimpleDateFormat(
            configLoader.getString("summary_log_path","job_fmt")).format(date.getTime())
        println("job start time : " + jobStartTime)
        Summary.setJobStartTime(jobStartTime)

        println(s"flag: $flag")

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        //val sparkConfigMap = configLoader.getString("minio", "bucket")

        val sparkBuilder = SparkSession
          .builder
          .appName(configLoader.getString("spark", "job_name"))
          .master(configLoader.getString("spark", "master"))

        val confStr = configLoader.getString("spark", "conf")

        val confAry = confStr.split(";").map(_.trim)
        for(i <- 0 until confAry.length) {
            val configKeyValue = confAry(i).split("=").map(_.trim)
            println("conf ===> " + configKeyValue(0) + " : " + configKeyValue(1))
            sparkBuilder.config(configKeyValue(0), configKeyValue(1))
        }

        val spark = sparkBuilder.getOrCreate()

        val configMap = spark.conf.getAll
        for ((k,v) <- configMap) {
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

        val vendorCodeConfMap = configLoader.getSectionByName("part_vendor_rules")

        vendorCodeConfMap.forEach((k: String, v: String) => {
            val vList = v.split(",")
            vendorCodeMap = vendorCodeMap + (k -> (vList(0), vList(1).toInt - 1, vList(2).toInt))
        })

        val dateCodeConfMap = configLoader.getSectionByName("part_datecode_rules")

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
            val wipDestPath = IoUtils.flatMinioFiles(spark,
                flag,
                wipPath,
                wipFileLmits)

            var wipSourceDf = IoUtils.getDfFromPath(spark, wipDestPath.toString, wipColumns, dataSeperator)
            val wipSourceDfCnt = wipSourceDf.count()

            wipSourceDf = wipSourceDf.distinct()
              .withColumn("product", regexp_replace($"product", "\t", " "))
              .withColumn("scantime1", unix_timestamp(trim($"scantime"),
                  configLoader.getString("log_prop", "wip_dt_fmt")).cast(TimestampType))
              .drop("scantime")
              .withColumnRenamed("scantime1", "scantime")
              .orderBy("scantime")
              .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

            val wipSourceDfDistCnt = wipSourceDf.count()
//            Summary.setMasterFilesRows(IoUtils.getFilesRows(spark, wipDestPath))
            Summary.setMasterFilesNameList(IoUtils.getFilesNameList(spark, wipDestPath))

            val wipPartsDestPath = IoUtils.flatMinioFiles(spark,
                flag,
                wipPartsPath,
                wipPartsFileLmits)

            var wipPartsSourceDf = IoUtils.getDfFromPath(spark, wipPartsDestPath.toString, wipPartsColumns, dataSeperator)
            val wipPartsSourceDfCnt = wipPartsSourceDf.count()
            wipPartsSourceDf = wipPartsSourceDf.distinct()
              .withColumn("scantime1", unix_timestamp(trim($"scantime"),
                  configLoader.getString("log_prop", "wip_parts_dt_fmt")).cast(TimestampType))
              .drop("scantime")
              .withColumnRenamed("scantime1", "scantime")
              .orderBy("scantime")

            //需要改為查表之動態欄位
            val vendorUdf = udf((partName: String, partSn: String) => {
                if (vendorCodeMap.contains(partName)) {
                    val source = vendorCodeMap(partName)

                    if (partSn.length > source._3) {
                        partSn.substring(source._2, source._3)
                    }
                    else {
                        partSn
                    }
                }
                else
                    partSn
            })

            val dateUdf = udf((partName: String, partSn: String) => {
                if (dateCodeMap.contains(partName)) {
                    val source = dateCodeMap(partName)

                    if (partSn.length > source._3) {
                        partSn.substring(source._2, source._3)
                    } else {
                        partSn
                    }
                }
                else
                    partSn
            })

            val wipPartsDetailSourceDf = wipPartsSourceDf
              .withColumn("vendor_code", vendorUdf($"part", $"partsn"))
              .withColumn("date_code", dateUdf($"part", $"partsn"))
              .withColumn("vendor_date_code", concat($"vendor_code", lit("_"), $"date_code"))
              .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

            import org.apache.spark.sql.functions.udf

            val time_group_duration_min = 10

            //performance issue of DateTimeFormatter
            val startTimeGroupUdf = udf((startTimeStr: String)
            => {
                val bobcatDatetimeFormatter = DateTimeFormatter
                  .ofPattern(configLoader.getString("log_prop", "bobcat_dt_fmt"), Locale.US)

                val startTime = LocalDateTime
                  .parse(startTimeStr, bobcatDatetimeFormatter)

                startTime
                  .minusMinutes(startTime.getMinute % time_group_duration_min)
                  .withSecond(0)
                  .format(bobcatDatetimeFormatter)
            }
            )

            val endTimeGroupUdf = udf((startTimeStr: String)
            => {
                val bobcatDatetimeFormatter = DateTimeFormatter
                  .ofPattern(configLoader.getString("log_prop", "bobcat_dt_fmt"), Locale.US)

                val startTime = LocalDateTime
                  .parse(startTimeStr, bobcatDatetimeFormatter)

                startTime
                  .plusMinutes(time_group_duration_min - (startTime.getMinute % time_group_duration_min))
                  .withSecond(0)
                  .format(bobcatDatetimeFormatter)
            }
            )

            val symptomArrayUdf = udf((symptom: String)
            => {
                symptom
                  .split(";")
                  .map(_.trim)
            }
            )

            val machineUdf = udf((station: String, stationCode: String, machine: String) => {
                if (machine.isEmpty) {
                    if (stationCode.isEmpty) {
                        station
                    } else {
                        stationCode
                    }
                } else {
                    machine
                }

            })

            val wipPartsSourceDfDistCnt = wipPartsSourceDf.count()
//            Summary.setDetailFilesRows(IoUtils.getFilesRows(spark, wipPartsDestPath))
            Summary.setDetailFilesNameList(IoUtils.getFilesNameList(spark, wipPartsDestPath))

            val bobcatDestPaths = IoUtils.flatMinioFiles(spark,
                flag,
                bobcatPath,
                bobcatFileLmits)

            var bobcatSourceDf = IoUtils.getDfFromPath(spark, bobcatDestPaths.toString, bobcatColumns, dataSeperator)
            val bobcatSourceDfCnt = bobcatSourceDf.count()

            bobcatSourceDf = bobcatSourceDf.distinct()
                .withColumn("factory", lit(factory))
                .withColumn("start_time_new", when(trim($"start_time") === "", $"end_time").otherwise($"start_time"))
                .drop("start_time")
                .withColumnRenamed("start_time_new", "start_time")
                .withColumn("ori_symptom", $"symptom")
                .withColumn("start_time1",
                    unix_timestamp(trim($"start_time"),
                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
                .withColumn("start_time_group",
                    unix_timestamp(startTimeGroupUdf(trim($"start_time")),
                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
                .withColumn("end_time_group",
                    unix_timestamp(endTimeGroupUdf(trim($"start_time")),
                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
                .withColumn("end_time1",
                    unix_timestamp(trim($"end_time"),
                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
                .withColumn("uploadtime1",
                    unix_timestamp(trim($"uploadtime"),
                        configLoader.getString("log_prop", "bobcat_dt_fmt")).cast(TimestampType))
                .withColumn("machine_1", machineUdf($"station", $"stationcode", $"machine"))
                .drop("start_time")
                .drop("end_time")
                .drop("uploadtime")
                .drop("machine")
                .withColumnRenamed("start_time1", "start_time")
                .withColumnRenamed("end_time1", "end_time")
                .withColumnRenamed("uploadtime1", "uploadtime")
                .withColumnRenamed("machine_1", "machine")
                .persist(StorageLevel.MEMORY_AND_DISK_SER)
            //
            //        bobcatSourceDf.where($"symptom" =!= "").show(1000, false)
            //
            val bobcatSourceDfDistCnt = bobcatSourceDf.count()
//            Summary.setTestFilesRows(IoUtils.getFilesRows(spark, bobcatDestPaths))
            Summary.setTestFilesNameList(IoUtils.getFilesNameList(spark, bobcatDestPaths))

            println("wipSourceDf : " + wipSourceDfCnt)
            println("wipPartsDetailSourceDf : " + wipPartsSourceDfCnt)
            println("bobcatSourceDf : " + bobcatSourceDfCnt)

            println("wipSourceDf (distinct) : " + wipSourceDfDistCnt)
            println("wipPartsDetailSourceDf (distinct) : " + wipPartsSourceDfDistCnt)
            println("bobcatSourceDf (distinct) : " + bobcatSourceDfDistCnt)

            Summary.setMasterFilesTotalRows(wipSourceDfCnt)
            Summary.setDetailFilesTotalRows(wipPartsSourceDfCnt)
            Summary.setTestFilesTotalRows(bobcatSourceDfCnt)

            // dist
            Summary.setMasterFilesTotalRowsDist(wipSourceDfDistCnt)
            Summary.setDetailFilesTotalRowsDist(wipPartsSourceDfDistCnt)
            Summary.setTestFilesTotalRowsDist(bobcatSourceDfDistCnt)

            Summary.setMasterTotalRowsInCockroachdb(wipSourceDfCnt)
            Summary.setDetailTotalRowsInCockroachdb(wipPartsSourceDfCnt)
            Summary.setTestTotalRowsInCockroachdb(bobcatSourceDfCnt)

            //dist
            Summary.setMasterTotalRowsDistInCockroachdb(wipSourceDfDistCnt)
            Summary.setDetailTotalRowsDistInCockroachdb(wipPartsSourceDfDistCnt)
            Summary.setTestTotalRowsDistInCockroachdb(bobcatSourceDfDistCnt)

            println(wipSourceDf.count() + ", " + wipSourceDf.distinct().count())
            wipSourceDf.show()
            println("wipSourceDf 筆數 : " + wipSourceDf.count())

            println(wipPartsDetailSourceDf.count() + ", " + wipPartsDetailSourceDf.distinct().count())
            wipPartsDetailSourceDf.show(false)
            println("wipPartsDetailSourceDf 筆數 : " + wipPartsDetailSourceDf.count())

            println(bobcatSourceDf.count() + ", " + bobcatSourceDf.distinct().count())
            bobcatSourceDf.show()
            println("bobcatSourceDf 筆數 : " + bobcatSourceDf.count())


            //將資料儲存進Cockroachdb
            println("saveToCockroachdb --> wipSourceDf")
            IoUtils.saveToCockroachdb(wipSourceDf.withColumn("factory", lit(factory)),
                configLoader.getString("log_prop", "wip_table"),
                numExecutors)

            println("saveToCockroachdb --> wipPartsDetailSourceDf")
            IoUtils.saveToCockroachdb(wipPartsDetailSourceDf.withColumn("factory", lit(factory)),
                configLoader.getString("log_prop", "wip_parts_table"),
                numExecutors)

            println("saveToCockroachdb --> bobcatSourceDf")
            IoUtils.saveToCockroachdb(bobcatSourceDf,
                configLoader.getString("log_prop", "bobcat_table"),
                numExecutors)

            //first row by wipSource
            println("===> first row by wipSource")

//            wipSourceDf.printSchema()

            val wipSourceFirstTimeList = wipSourceDf
                            .select("scantime")
                            .limit(1)
                            .withColumn("scantime2", $"scantime".cast(StringType))
                            .select("scantime2")
                            .rdd
                            .map(r => r(0))
                            .collect
                            .toList
                            .asInstanceOf[List[String]]
            println("===> wipSourceFirstTimeList(0) : " + wipSourceFirstTimeList(0))
            Summary.setMasterFirstRowTimeInCockroachdb(wipSourceFirstTimeList(0))

            //last row by wipSource
            println("===> last row by wipSource")
            val wipSourceLastTimeList = wipSourceDf
              .select("scantime")
              .orderBy($"scantime".desc)
              .limit(1)
              .withColumn("scantime2", $"scantime".cast(StringType))
              .select("scantime2")
              .rdd
              .map(r => r(0))
              .collect
              .toList
              .asInstanceOf[List[String]]
            println("===> wipSourceLastTimeList(0) : " + wipSourceLastTimeList(0))
            Summary.setMasterLastRowTimeInCockroachdb(wipSourceLastTimeList(0))

            //first row by wipPartsDetailSource
            println("===> first row by wipPartsDetailSource")
            val wipPartsDetailSourceFirstTimeList: List[String] = wipPartsDetailSourceDf
              .select("scantime")
              .limit(1)
              .withColumn("scantime2", $"scantime".cast(StringType))
              .select("scantime2")
              .rdd
              .map(r => r(0))
              .collect
              .toList
              .asInstanceOf[List[String]]
            println("===> wipPartsDetailSourceFirstTimeList(0) : " + wipPartsDetailSourceFirstTimeList(0))
            Summary.setDetailFirstRowTimeInCockroachdb(wipPartsDetailSourceFirstTimeList(0))

            //last row by wipPartsDetailSource
            println("===> last row by wipPartsDetailSource")
            val wipPartsDetailSourceLastTimeList: List[String] = wipPartsDetailSourceDf
              .select("scantime")
              .orderBy($"scantime".desc)
              .limit(1)
              .withColumn("scantime2", $"scantime".cast(StringType))
              .select("scantime2")
              .rdd
              .map(r => r(0))
              .collect
              .toList
              .asInstanceOf[List[String]]
            println("===> wipPartsDetailSourceLastTimeList(0) : " + wipPartsDetailSourceLastTimeList(0))
            Summary.setDetailLastRowTimeInCockroachdb(wipPartsDetailSourceLastTimeList(0))

//            bobcatSourceDf.printSchema()

            //first row by bobcatSource
            println("===> first row by bobcatSource")
            val bobcatSourceFirstTimeList: List[String] = bobcatSourceDf
              .select("start_time")
              .limit(1)
              .withColumn("start_time2", $"start_time".cast(StringType))
              .select("start_time2")
              .rdd
              .map(r => r(0))
              .collect
              .toList
              .asInstanceOf[List[String]]
            println("===> bobcatSourceFirstTimeList(0) : " + bobcatSourceFirstTimeList(0))
            Summary.setTestFirstRowTimeInCockroachdb(bobcatSourceFirstTimeList(0))

            //last row by wipPartsDetailSource
            println("===> last row by wipPartsDetailSource")
            val bobcatSourceLastTimeList: List[String] = bobcatSourceDf
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
            println("===> bobcatSourceLastTimeList(0) : " + bobcatSourceLastTimeList(0))
            Summary.setTestLastRowTimeInCockroachdb(bobcatSourceLastTimeList(0))


            println("===> 抓取和該次測試有關的組裝資料")

            ////////////////////////////
            //抓取和該次測試有關的組裝資料//
            ////////////////////////////
            val bobcatSnArr = bobcatSourceDf
              .select("sn")
              .distinct()
              .rdd
              .map(r => r(0).asInstanceOf[String])
              .collect

            // println(bobcatSnArr.size)

            val bobcatTmpDbPredicates = Array[String]("sn in (" + bobcatSnArr.map(s => "'" + s + "'").mkString(",") + ") and ori_symptom = symptom")

            val bobcatTmpDbDf = IoUtils
              .getDfFromCockroachdb(spark, configLoader.getString("log_prop", "bobcat_table"), bobcatTmpDbPredicates)
              .select(bobcatSourceDf.columns.map(name => col(name)): _*)
              .distinct()
              .orderBy("start_time")

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

            val failCntSpec = Window
                .partitionBy("sn")
                .orderBy("sn", "start_time", "symptom", "desc_", "stationcode", "machine", "start_time", "end_time", "uploadtime")

            //by sn 重新累計 fail_cnt
            val bobcatDbAggTmpDf_2 = bobcatDbAggTmpDf_1
                .withColumn("fail_cnt_1", sum(bobcatDbAggTmpDf_1("is_true_fail")).over(failCntSpec).cast(IntegerType))
                .withColumn("is_true_fail_1", when($"fail_cnt_1" >= 1, 1).otherwise(0))

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

            //        bobcatDbAggDf.where("sn = 'CN93D870M8'").select("*").show(false)

            //todo is_symptom_fail 需全面update

            println("saveToCockroachdb --> bobcatDbAggDf")
            IoUtils.saveToCockroachdb(bobcatDbAggDf,
                configLoader.getString("log_prop", "bobcat_table"),
                numExecutors)

            val wipDbPredicates = Array[String]("sn in (" + bobcatSnArr.map(s => "'" + s + "'").mkString(",") + ")")

            val wipDbDf = IoUtils
                .getDfFromCockroachdb(spark, configLoader.getString("log_prop", "wip_table"), wipDbPredicates)
                .withColumnRenamed("scantime", "wip_scantime")
                .persist(StorageLevel.MEMORY_AND_DISK_SER)

            val wipPartsIdArr = wipDbDf
                .select("id")
                .distinct()
                .rdd
                .map(r => r(0).asInstanceOf[String])
                .collect()

            val wipPartsDetailDbPredicates = Array[String]("id in (" + wipPartsIdArr.map(s => "'" + s + "'").mkString(",") + ")")

            val wipPartsDetailDbDf = IoUtils
                .getDfFromCockroachdb(spark, configLoader.getString("log_prop", "wip_parts_table"), wipPartsDetailDbPredicates)
                .persist(StorageLevel.MEMORY_AND_DISK_SER)

            println("===> 整合工單及組裝資訊")

            ////////////////////
            //整合工單及組裝資訊//
            ////////////////////
            val lineUdf = udf((floor: String, line: String) => {
                if (line.isEmpty) {
                    floor
                } else {
                    line
                }

            })

            val wipPartFullDbDf = wipPartsDetailDbDf
                .join(wipDbDf, Seq("id", "factory"))
                .withColumn("line_1", lineUdf($"floor", $"line"))
                .drop("line")
                .withColumnRenamed("line_1", "line")
                .persist(StorageLevel.MEMORY_AND_DISK_SER)

            wipPartFullDbDf.show()
            println("wipPartFullDbDf 筆數 : " + wipPartFullDbDf.count())

            ///////////////////
            //分析測試、組裝資料//
            ///////////////////

            println("===> 分析測試、組裝資料")

            val mariadbUtils = new MariadbUtils()

            val bobcatSourceSize = bobcatSourceDf
              .select("start_time_group", "end_time_group")
              .distinct()
              .orderBy("start_time_group")
              .rdd
              .map(r => (r(0).asInstanceOf[java.sql.Timestamp], r(1).asInstanceOf[java.sql.Timestamp]))
              .collect()
              .size

            println("bobcatSourceSize : " + bobcatSourceSize)

            var bobcatSourceDfCount = 0
            var checkCountFlag = "ready"
            var totalRowsInMysqldb: Long = 0

            bobcatSourceDf
              .select("start_time_group", "end_time_group")
              .distinct()
              .orderBy("start_time_group")
              .rdd
              .map(r => (r(0).asInstanceOf[java.sql.Timestamp], r(1).asInstanceOf[java.sql.Timestamp]))
              .collect()
              .foreach(timeTuple => {

                  val start_time_group = timeTuple._1
                  val end_time_group = timeTuple._2

                  println("start_time_group : " + start_time_group)
                  println("end_time_group : " + end_time_group)

                  val bobcatSnPartialArr = bobcatSourceDf
                      .where($"start_time_group" === start_time_group)
                      .select("sn")
                      .distinct()
                      .rdd
                      .map(r => r(0).asInstanceOf[String])
                      .collect

                  val bobcatPartialDf = bobcatDbAggDf
                      .where((($"istestfail" === 0 and $"fail_cnt" === 0) or ($"istestfail" === 1 and $"fail_cnt" === 1)) and $"start_time_group" === start_time_group)
                      .orderBy("sn", "start_time")

                  bobcatSourceDfCount = bobcatSourceDfCount + 1
                  println("bobcatSourceDfCount : " + bobcatSourceDfCount)
                  if (bobcatSourceDfCount == 1) {
                      checkCountFlag = "first"
                  } else if (bobcatSourceDfCount == bobcatSourceSize) {
                      checkCountFlag = "last"
                  } else {

                  }

                  if(configLoader.getString("mariadb", "database").equals("rca-ipbd-lx-monitor")) {

                      println("rca-ipbd-lx-monitor join logical")

                      val wipPartFullPartialDf = wipPartFullDbDf
                          .where($"scantime" <= end_time_group and $"sn".isin(bobcatSnPartialArr: _*))
                          .orderBy("sn", "wip_scantime", "scantime")

                      println(start_time_group)
                      println("bobcatPartialDf")
                      bobcatPartialDf.show()
                      println("wipPartFullPartialDf")
                      wipPartFullPartialDf.show()

                      val analyzer = new Analyzer()

                      val insertTotalRowsInMysqldb = analyzer.analysis(
                          spark,
                          mariadbUtils,
                          start_time_group.toLocalDateTime,
                          failCondition,
                          wipPartFullPartialDf,
                          bobcatPartialDf,
                          numExecutors,
                          checkCountFlag)

                      println("insertTotalRowsInMysqldb : " + insertTotalRowsInMysqldb)

                      totalRowsInMysqldb = totalRowsInMysqldb + insertTotalRowsInMysqldb

                  } else {
                      val wipPartFullPartialDf = wipPartFullDbDf
                          .where($"scantime" <= end_time_group and $"wip_scantime" <= $"scantime" and $"sn".isin(bobcatSnPartialArr: _*))
                          .orderBy("sn", "wip_scantime", "scantime")

                      println(start_time_group)
                      println("bobcatPartialDf")
                      bobcatPartialDf.show()
                      println("wipPartFullPartialDf")
                      wipPartFullPartialDf.show()

                      val analyzer = new Analyzer()

                      val insertTotalRowsInMysqldb = analyzer.analysis(
                          spark,
                          mariadbUtils,
                          start_time_group.toLocalDateTime,
                          failCondition,
                          wipPartFullPartialDf,
                          bobcatPartialDf,
                          numExecutors,
                          checkCountFlag)

                      println("insertTotalRowsInMysqldb : " + insertTotalRowsInMysqldb)

                      totalRowsInMysqldb = totalRowsInMysqldb + insertTotalRowsInMysqldb
                  }
              })

//            Summary.setTotalRowsInMysqldb(totalRowsInMysqldb)

            println("===> 寫入新增product, floor, line")

            ///////////////////////////////
            //寫入新增product, floor, line//
            ///////////////////////////////
            val productFloorLineDbDf = mariadbUtils
              .getDfFromMariadb(spark, "product_floor_line")
              .drop("update_time")
              .select("product", "floor", "line")

            println("productFloorLineDbDf 筆數 : " + productFloorLineDbDf.count())
            productFloorLineDbDf.show()

            val productFloorLineFileDf = wipPartFullDbDf
              .select("product", "floor", "line")
              .distinct()

            println("productFloorLineFileDf 筆數 : " + productFloorLineFileDf.count())
            productFloorLineFileDf.show()

            val productFloorLineDiffDf = productFloorLineFileDf
              .except(productFloorLineDbDf)

            println("productFloorLineDiffDf 筆數 : " + productFloorLineDiffDf.count())
            productFloorLineDiffDf.show()

            if(productFloorLineDiffDf.count() > 0) {
                println("===> 開始將productFloorLineDiffDf資料寫入Mariadb")
                mariadbUtils.saveToMariadb(
                    productFloorLineDiffDf
                      .select("product", "floor", "line")
                      .distinct(),
                    "product_floor_line",
                    numExecutors)
            } else {
                println("===> 無資料(productFloorLineDiffDf)寫入Mariadb")
            }

            date = new java.util.Date();
            val jobEndTime: String = new SimpleDateFormat(configLoader.getString("summary_log_path","job_fmt")).format(date.getTime())
            println("job end time : " + jobEndTime)
            Summary.setJobEndTime(jobEndTime)
            Summary.save(spark)
        } catch {
            case ex: FileNotFoundException => {
                // ex.printStackTrace()
                println("===> FileNotFoundException !!!")
            }
        }

    }

}
