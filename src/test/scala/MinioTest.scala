package com.foxconn.iisd.bd.test.rca.utils.db

import java.net.URI
import java.time.format.DateTimeFormatter
import java.util.Date

import com.foxconn.iisd.bd.test.config.ConfigLoaderTest
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.Map

object MinioTest {

    val configLoader = new ConfigLoaderTest("""conf/default.yaml""")
    val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val flag = new Date().getTime().toString

        val spark = SparkSession
            .builder
            .appName(configLoader.getString("spark", "job_name"))
            .master(configLoader.getString("spark", "master"))
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .getOrCreate()

        var logPathSection = "local_log_path"
        val isFromMinio = configLoader.getString("general", "from_minio").toBoolean

        if(isFromMinio) {
            logPathSection = "minio_log_path"
        }

        val endpoint = configLoader.getString("minio", "endpoint")
        val accessKey = configLoader.getString("minio", "accessKey")
        val secretKey = configLoader.getString("minio", "secretKey")
        val bucket = configLoader.getString("minio", "bucket")

        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)

        import spark.implicits._

        val factory = configLoader.getString("general", "factory")

        val failCondition: Int = configLoader.getString("analysis", "fail_condition").toInt

        //s3a://" + bucket + "/
        val wipPath = configLoader.getString(logPathSection, "wip_path")
        val wipPartsPath = configLoader.getString(logPathSection, "wip_parts_path")
        val bobcatPath = configLoader.getString(logPathSection, "bobcat_path")

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
            vendorCodeMap = vendorCodeMap + (k -> (vList(0), vList(1).toInt, vList(2).toInt))
        })

        val dateCodeConfMap = configLoader.getSection("part_datecode_rules")

        dateCodeConfMap.forEach((k: String, v: String) => {
            val vList = v.split(",")
            dateCodeMap = dateCodeMap + (k -> (vList(0), vList(1).toInt, vList(2).toInt))
        })

//        val df = getDfFromMinio(spark, wipPath, wipColumns, dataSeperator)
//
//        df.show()

        print(wipPath)

        flatMinioFiles(spark,
            flag,
            wipPath, wipColumns, dataSeperator)

        flatMinioFiles(spark,
            flag,
            wipPartsPath, wipPartsColumns, dataSeperator)

        flatMinioFiles(spark,
            flag,
            bobcatPath, bobcatColumns, dataSeperator)
    }

    def flatMinioFiles(spark: SparkSession, flag:String, srcPathStr: String, columns: String, dataSeperator: String): (Path, Path) = {
        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)

        var count = 0

        println(srcPathStr)

        val srcPath = new Path(srcPathStr)
        val destPath = new Path(new Path(srcPath.getParent, s"${srcPath.getName}_TMP"), flag)
        val zeroPath = new Path(new Path(srcPath.getParent, s"${srcPath.getName}_TMP_ZERO"), flag)

//        if(!fileSystem.exists(destPath)){
//            fileSystem.mkdirs(destPath)
//        }
//
//        if(!fileSystem.exists(zeroPath)){
//            fileSystem.mkdirs(zeroPath)
//        }

        val wipPathFiles = fileSystem.listFiles(srcPath, true)
        while (count < 1 && wipPathFiles.hasNext()) {
            val file = wipPathFiles.next()

            val filename = file.getPath.getName
            val tmpFilePath = new Path(destPath, filename)
            val tmpZeroFilePath = new Path(zeroPath, filename)

            if(file.getLen > 0) {
//                println(s"${file.getPath} -> ${tmpFilePath.toString} : ${file.getLen}")
//                fileSystem.rename(file.getPath, tmpFilePath)

                println(file.getPath)

                val df = getDfFromMinio(spark, file.getPath.toString, columns, dataSeperator)

                df.show()

                count = count + 1

//            } else {
//                println(s"${file.getPath} -> ${tmpFilePath.toString} : ${file.getLen}")
////                fileSystem.rename(file.getPath, tmpZeroFilePath)
            }
        }

        return (destPath, zeroPath)
    }

    def getDfFromFile(spark: SparkSession, path: String, columns: String, dataSeperator: String): DataFrame = {

        val schema = StructType(columns
            .split(",")
            .map(fieldName => StructField(fieldName,StringType, true)))

        val rdd = spark
            .sparkContext
            .textFile(path)
            .map(_.replace("'", "、"))
            .map(_.split(dataSeperator, schema.fields.length).map(field => {
                if(field.isEmpty)
                    ""
                else
                    field.trim
            }))
            .map(p => Row(p: _*))

        return spark.createDataFrame(rdd, schema)
    }

    def getDfFromMinio(spark: SparkSession, path: String, columns: String, dataSeperator: String): DataFrame = {

        val schema = StructType(columns
            .split(",")
            .map(fieldName => StructField(fieldName,StringType, true)))

        val rdd = spark
            .sparkContext
            .textFile(path)
            .map(_.replace("'", "、"))
            .map(_.split(dataSeperator, schema.fields.length).map(field => {
                if(field.isEmpty)
                    ""
                else
                    field.trim
            }))
            .map(p => Row(p: _*))

        return spark.createDataFrame(rdd, schema)
    }
}
