package com.foxconn.iisd.bd.rca.utils

import java.io._
import java.net.URI
import java.nio.file.{Files, Paths}
import java.sql.DriverManager
import java.util.Properties

import com.foxconn.iisd.bd.rca.XWJKernelEngine.configLoader
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.util
import org.apache.hadoop.fs.FileUtil


import scala.io.Source

object IoUtils {

    def flatMinioFiles(spark: SparkSession, flag:String, srcPathStr: String, fileLimits: Integer): Path = {
        var count = 0

        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)

        val srcPath = new Path(srcPathStr)
        val destPath = new Path(new Path(srcPath.getParent, s"${srcPath.getName}_TMP"), flag)

        if(!fileSystem.exists(destPath)){
            fileSystem.mkdirs(destPath)
        }

        //try {
            val wipPathFiles = fileSystem.listFiles(srcPath, true)
            while (count < fileLimits && wipPathFiles.hasNext()) {
                val file = wipPathFiles.next()

                val filename = file.getPath.getName
                val tmpFilePath = new Path(destPath, filename)


                if (file.getLen > 0) {
                  FileUtil.copy(fileSystem, file.getPath, fileSystem, tmpFilePath, false, true, spark.sparkContext.hadoopConfiguration)
//                    println(s"[MOVE] ${file.getPath} -> ${tmpFilePath.toString} : ${file.getLen}")
//                    fileSystem.rename(file.getPath, tmpFilePath)

                    count = count + 1
                    Thread.sleep(2000)

                }
            }
        /*} catch {
            case ex: FileNotFoundException => {
                //                ex.printStackTrace()
                println("===> FileNotFoundException !!!")
            }
        }*/
        return destPath
    }

    def getDfFromPath(spark: SparkSession, path: String, columns: String, dataSeperator: String): DataFrame = {

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
              else if(field.contains("\003"))//控制字元不濾掉空白
                  field
              else
                  field.trim

          }))
          .map(p => Row(p: _*))

        rdd.take(10).map(println)

        return spark.createDataFrame(rdd, schema)
    }

//    def getDfFromCockroachdb(spark: SparkSession, table: String): DataFrame = {
//
//        val cockroachdbUrl = configLoader.getString("cockroachdb", "conn_str")
//        val cockroachdbConnectionProperties = new Properties()
//
//        cockroachdbConnectionProperties.put(
//            "user",
//            configLoader.getString("cockroachdb", "username")
//        )
//
//        cockroachdbConnectionProperties.put(
//            "password",
//            configLoader.getString("cockroachdb", "password")
//        )
//
//        cockroachdbConnectionProperties.put(
//            "sslmode",
//            configLoader.getString("cockroachdb", "sslmode")
//        )
//
//        return spark.read.jdbc(cockroachdbUrl, table, cockroachdbConnectionProperties)
//    }

    def getDfFromCockroachdb(spark: SparkSession, table: String, predicates: Array[String]): DataFrame = {

        val cockroachdbUrl = configLoader.getString("cockroachdb", "conn_str")
        val cockroachdbConnectionProperties = new Properties()

        cockroachdbConnectionProperties.put(
            "user",
            configLoader.getString("cockroachdb", "username")
        )

        cockroachdbConnectionProperties.put(
            "password",
            configLoader.getString("cockroachdb", "password")
        )

        cockroachdbConnectionProperties.put(
            "sslmode",
            configLoader.getString("cockroachdb", "sslmode")
        )

        return spark.read.jdbc(cockroachdbUrl, table, predicates, cockroachdbConnectionProperties)
    }

    def saveToCockroachdb(df: DataFrame, table: String, numExecutors: Int): Unit = {
        val cockroachdbUrl = configLoader.getString("cockroachdb", "conn_str")
        val cockroachdbConnectionProperties = new Properties()

        cockroachdbConnectionProperties.put(
            "user",
            configLoader.getString("cockroachdb", "username")
        )

        cockroachdbConnectionProperties.put(
            "password",
            configLoader.getString("cockroachdb", "password")
        )

        cockroachdbConnectionProperties.put(
            "sslmode",
            configLoader.getString("cockroachdb", "sslmode")
        )

        cockroachdbConnectionProperties.put(
            "allowEncodingChanges",
            "true"
        )

        val sqlPrefix =
            "UPSERT INTO " + table +
              "(" + df.columns.mkString(",") + ")" +
              " VALUES "

        val batchSize = 3000
        val batchLength = 600000
        val repartitionSize = numExecutors

        df.distinct.rdd.repartition(repartitionSize).foreachPartition{

            partition => {

                val conn = DriverManager.getConnection(
                    cockroachdbUrl,
                    cockroachdbConnectionProperties)

                conn.setAutoCommit(false)

                var runCount = 0
                var count = 0
                var sql = sqlPrefix

                partition.foreach { r =>
                    count += 1

                    val values = r.mkString("'", "','", "'")
                      .replaceAll("'null'", "null")
                      .replaceAll("\"null\"", "null")
                      .replaceAll("'ARRAY\\[", "ARRAY[")
                      .replaceAll("\\]'", "]")

                    sql = sql + "(" + values + ") ,"

                    if(sql.length >= batchLength || count == batchSize){
                        runCount = runCount + 1

                        try {
                            println(s"[$runCount]: ${sql.length}")
                            conn.createStatement().execute(sql.substring(0, sql.length - 1))
                        } catch {
                            case e: Exception => {
                                println(s"${sql.substring(0, sql.length - 1)}")
                                e.printStackTrace()
                            }
                        }

                        count = 0
                        sql = sqlPrefix

                        conn.commit()

                        conn.createStatement().clearWarnings()
                    }
                }

                if(count > 0) {
                    conn.createStatement().execute(sql.substring(0, sql.length - 1))
                }

                conn.commit()

                conn.close()
            }
        }
    }

//    def getFilesRows(spark: SparkSession, minioTempPath: Path): util.HashMap[String, Integer] = {
//        var filesRowsMap = new util.HashMap[String, Integer]
//        try {
//            val fileSystem = FileSystem.get(URI.create(minioTempPath.toString), spark.sparkContext.hadoopConfiguration)
//            val pathFiles = fileSystem.listFiles(minioTempPath, true)
//            var count = 0
//            while (pathFiles.hasNext()) {
//                val file = pathFiles.next()
//                val filename = file.getPath.getName
//                val tmpFilePath = new Path(minioTempPath, filename)
//                val inputStream = fileSystem.open(tmpFilePath)
//                val reader = new BufferedReader(new InputStreamReader(inputStream))
//                var line = reader.readLine
//                var rows = 0
//                while(line != null) {
//                    rows = rows + 1
//                    line = reader.readLine()
//                }
//
//                println("rows: " + rows)
//                filesRowsMap.put(filename, rows)
//
//                if(reader != null){
//                    reader.close()
//                }
//
//                count = count + 1
//                Thread.sleep(2000)
//            }
//            println("count: " + count)
//
//        } catch {
//            case ex: Exception => {
//                println("exception : " + ex.getMessage)
//            }
//        }
//        filesRowsMap
//    }

    def getFilesNameList(spark: SparkSession, minioTempPath: Path): util.ArrayList[String] = {
        var filesNameList = new util.ArrayList[String]
        try {
            val fileSystem = FileSystem.get(URI.create(minioTempPath.getParent.toString), spark.sparkContext.hadoopConfiguration)
            val pathFiles = fileSystem.listFiles(minioTempPath, true)
            while (pathFiles.hasNext()) {
                val file = pathFiles.next()
                val filename = file.getPath.getName
                val tmpFilePath = new Path(minioTempPath, filename)
                filesNameList.add(tmpFilePath.toString)

                Thread.sleep(2000)
            }

        } catch {
            case ex: Exception => {
                println("exception : " + ex.getMessage)
            }
        }
        filesNameList
    }

    def saveSummaryFileToMinio(spark: SparkSession,
                               summaryJsonStr: String): Unit = {

        val outputPathStr = configLoader.getString("summary_log_path", "data_base_path")
        val tag = configLoader.getString("summary_log_path", "tag")
        val fileExtension = configLoader.getString("summary_log_path", "file_extension")
        val bucket = configLoader.getString("minio", "bucket")
        println("bucket : " + bucket)
        println("bucket UpperCase : " + bucket.toUpperCase())

        val outputPath = new Path(outputPathStr)
        val fileSystem = FileSystem.get(URI.create(outputPath.getParent.toString), spark.sparkContext.hadoopConfiguration)
        if(!fileSystem.exists(outputPath)){
            fileSystem.mkdirs(outputPath)
        }

        import java.time.LocalDate
        val yearStr: String = LocalDate.now.getYear.toString
        val day = LocalDate.now.getDayOfMonth
        val month = LocalDate.now.getMonthValue
        var monthStr: String = ""
        if(month < 10) {
            monthStr = "0" + month.toString
        } else {
            monthStr = month.toString
        }
        var dayStr: String = ""
        if(day < 10) {
            dayStr = "0" + day.toString
        } else {
            dayStr = day.toString
        }
        val srcPath = new Path(outputPathStr)
        val destPath = new Path(new Path(srcPath, s"${tag}"), s"${yearStr}"+s"${monthStr}")
        if(!fileSystem.exists(destPath)){
            fileSystem.mkdirs(destPath)
        }
        import java.io._
        var output:FSDataOutputStream = null
        var fileInput:FSDataInputStream = null
        val builder = StringBuilder.newBuilder
        if(fileSystem.exists(new Path(destPath.toString + "/" + bucket.toUpperCase() + "_" + tag + "_" + yearStr + monthStr + dayStr + "." + fileExtension))) {
            fileInput = fileSystem.open(new Path(destPath.toString + "/" + bucket.toUpperCase() + "_" + tag + "_" + yearStr + monthStr + dayStr + "." + fileExtension))
            Source.fromInputStream(new BufferedInputStream(fileInput)).getLines().foreach { line => builder.append(line.toString + "\n") }
        }
        output = fileSystem.create(new Path(destPath.toString + "/" + bucket.toUpperCase() + "_" + tag + "_" + yearStr + monthStr + dayStr + "." + fileExtension))
        val writer = new PrintWriter(output)
        try {
            if(fileInput != null) {
                writer.write(builder.toString())
            }
            writer.write(summaryJsonStr)
        } catch {
            case ex: Exception => {
                println("===> Exception")
            }
        }
        finally {
            writer.close()
        }
    }

}
