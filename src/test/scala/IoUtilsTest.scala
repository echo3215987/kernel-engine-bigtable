package com.foxconn.iisd.bd.test.rca.utils

import java.io.FileNotFoundException
import java.net.URI
import java.sql.DriverManager
import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import com.foxconn.iisd.bd.test.rca.KernelEngineTest.configLoader

object IoUtilsTest {

    def flatMinioFiles(spark: SparkSession, flag:String, srcPathStr: String, fileLimits: Integer): Path = {

        var count = 0;

        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)

        val srcPath = new Path(srcPathStr)
        val destPath = new Path(new Path(srcPath.getParent, s"${srcPath.getName}_TMP"), flag)
//        val zeroPath = new Path(new Path(srcPath.getParent, s"${srcPath.getName}_TMP_ZERO"), flag)

        if(!fileSystem.exists(destPath)){
            fileSystem.mkdirs(destPath)
        }

//        if(!fileSystem.exists(zeroPath)){
//            fileSystem.mkdirs(zeroPath)
//        }
        try {
            val wipPathFiles = fileSystem.listFiles(srcPath, true)
            while (count < fileLimits && wipPathFiles.hasNext()) {
                val file = wipPathFiles.next()

                val filename = file.getPath.getName
                val tmpFilePath = new Path(destPath, filename)
                //            val tmpZeroFilePath = new Path(zeroPath, filename)

                if (file.getLen > 0) {
                    println(s"[MOVE] ${file.getPath} -> ${tmpFilePath.toString} : ${file.getLen}")
                    fileSystem.rename(file.getPath, tmpFilePath)
                    count = count + 1
                    Thread.sleep(300)

                    //            } else {
                    //                println(s"[Delete] ${file.getPath}: ${file.getLen}")
                    //                fileSystem.deleteOnExit(file.getPath)
                    //            }
                }
            }
        } catch {
            case ex: FileNotFoundException => {
//                ex.printStackTrace()
                println("===> FileNotFoundException !!!")
            }
        }

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
                else
                    field.trim
            }))
            .map(p => Row(p: _*))

        rdd.take(10).map(println)

        return spark.createDataFrame(rdd, schema).distinct()
    }

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

        println(numExecutors)

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

                    val values = r.mkString("'", "','", "'").replaceAll("'null'", "null")

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
}
