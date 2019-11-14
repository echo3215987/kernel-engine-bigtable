package com.foxconn.iisd.bd.rca

import java.net.URI
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.Properties
import java.io.FileNotFoundException

import com.foxconn.iisd.bd.rca.SparkUDF.genStaionJsonFormat
import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.hadoop.fs._
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.io.Source

object _IoUtils {

    private def getCockroachdbUrl(): String = {
        return configLoader.getString("cockroachdb", "conn_str")
    }

    private def getCockroachdbDriver(): String = {
        return configLoader.getString("cockroachdb", "driver")
    }

    private def getCockroachdbSSLMode(): String = {
        return configLoader.getString("cockroachdb", "sslmode")
    }

    private def getCockroachdbUser(): String = {
        return configLoader.getString("cockroachdb", "username")
    }

    private def getCockroachdbPassword(): String = {
        return configLoader.getString("cockroachdb", "password")
    }

    private def getCockroachdbConnectionProperties(): Properties ={
        val _cockroachdbConnectionProperties = new Properties()

        _cockroachdbConnectionProperties.put(
            "user",
            configLoader.getString("cockroachdb", "username")
        )

        _cockroachdbConnectionProperties.put(
            "password",
            configLoader.getString("cockroachdb", "password")
        )

        _cockroachdbConnectionProperties.put(
            "sslmode",
            configLoader.getString("cockroachdb", "sslmode")
        )

        return _cockroachdbConnectionProperties
    }

    def flatMinioFiles(spark: SparkSession, flag:String, srcPathStr: String, fileLimits: Integer): Path = {
        var count = 0

        val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)

        val srcPath = new Path(srcPathStr)
        val destPath = new Path(new Path(srcPath.getParent, s"${srcPath.getName}_TMP"), flag)

        if(!fileSystem.exists(destPath)){
            fileSystem.mkdirs(destPath)
        }

        try {
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
        } catch {
            case ex: FileNotFoundException => {
                //                ex.printStackTrace()
                println("===> FileNotFoundException !!!")
            }
        }
        return destPath
    }

    def getDfFromPath(spark: SparkSession, path: String, columns: String, dataSeperator: String): DataFrame = {

        val schema = createStringSchema(columns)

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

    def getDfFromCockroachdb(spark: SparkSession, table: String, predicates: Array[String]): DataFrame = {
        return spark.read.jdbc(this.getCockroachdbUrl(), table, predicates, this.getCockroachdbConnectionProperties())
    }

    def getDfFromCockroachdb(spark: SparkSession, query: String, numPartitions: Int): DataFrame = {
        return spark.read.format("jdbc")
          .option("url", this.getCockroachdbUrl())
          .option("numPartitions", numPartitions)
//          .option("partitionColumn", primaryKey)
          .option("sslmode", this.getCockroachdbSSLMode())
          .option("user", this.getCockroachdbUser())
          .option("password", this.getCockroachdbPassword())
          .option("query", query)
          .load()
    }


    def getRowFromResultSet(resultSet: ResultSet, colCount: Int): Row ={
        var i : Int = 1
        var seq = Seq("")
        while(i <= colCount){
            if(i == 1)
                seq = Seq(resultSet.getString(i))
            else
                seq = seq :+ resultSet.getString(i)
            i += 1
        }

        Row.fromSeq(seq)
    }

    def getDfFromCockroachdb(spark: SparkSession, selectArr: List[String], columns: String, selectSqlColumnName:String, selectSql: String): DataFrame = {

        val schema = createStringSchema(columns)

        var df = spark.emptyDataFrame
        var i = 0
        for (whereSql <- selectArr){

            val conn = DriverManager.getConnection(
                this.getCockroachdbUrl,
                this.getCockroachdbConnectionProperties)

            conn.setAutoCommit(false)

            val rs = conn.createStatement().executeQuery(selectSql + whereSql)
            val columnCnt: Int = rs.getMetaData.getColumnCount

            val resultSetRow = Iterator.continually((rs.next(), rs)).takeWhile(_._1).map(
                r => {
                    getRowFromResultSet(r._2, columnCnt) // (ResultSet) => (spark.sql.Row)
                }).toList

            val rdd = spark.sparkContext.makeRDD(resultSetRow)

            conn.commit()
            conn.close()

            val tempDf = spark.createDataFrame(rdd, schema)
            if(i == 0)
                df = tempDf
            else
                df = df.union(tempDf)
            i = i + 1
        }
        df
    }

    def getDfFromCockroachdb(spark: SparkSession, columns: String, sql: String): DataFrame = {

        val schema = createStringSchema(columns)

        val conn = DriverManager.getConnection(
            this.getCockroachdbUrl,
            this.getCockroachdbConnectionProperties)

        conn.setAutoCommit(false)

        val rs = conn.createStatement().executeQuery(sql)
        val columnCnt: Int = rs.getMetaData.getColumnCount

        val resultSetRow = Iterator.continually((rs.next(), rs)).takeWhile(_._1).map(
            r => {
                getRowFromResultSet(r._2, columnCnt) // (ResultSet) => (spark.sql.Row)
            }).toList

        val rdd = spark.sparkContext.makeRDD(resultSetRow)

        conn.commit()
        conn.close()

        spark.createDataFrame(rdd, schema)
    }

    def saveToCockroachdb(df: DataFrame, table: String, numExecutors: Int): Unit = {

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
                    this.getCockroachdbUrl,
                    this.getCockroachdbConnectionProperties)

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

    //parse data type
    def castColumnDataType(col: String) = {
        var datatype = "string"
        if(col != null){
            try {
                if (col.indexOf(".") > 0) {
                    //float
                    var value = col.toFloat
                    if (value.isInstanceOf[Float]) {
                        datatype = "float"
                    }
                } else {
                    //int
                    var value = col.toInt
                    if (value.isInstanceOf[Int]) {
                        datatype = "int"
                    }
                }
            }catch{
                case ex: Exception => {
                    // ex.printStackTrace()
                    println("===> cast data type Exception !!!")
                }
            }
        }
        datatype
    }

    def createStringSchema(columnNames: String) = {
        StructType(columnNames
          .split(",")
          .map(fieldName => StructField(fieldName,StringType, true)))
    }

    //gen test_value, test_item_result, test_item_result_detail column select sql
    def genTestDetailItemSelectSQL(colName:String, item: Seq[String]) = {
        //jsonb_build_object('CpyCopy.2^DLTVGCal', test_value->>'CpyCopy.2^DLTVGCal', 'CpyCopy.1^DLTVGCal', test_value->>'CpyCopy.2^DLTVGCal') as test_value
        item.map(i=> "'"+i+"'," + "t2." + colName + "->>'" + i + "'").mkString("jsonb_build_object(",",",") as " + colName)
    }

    //gen where sql TODO:delete, no use
    def genTestDetailWhereSQL(product: String, station: Seq[String], item: Seq[String]) = {
        var whereSql = " where product = '" + product + "'"
        whereSql = whereSql + " and (" + station.map(s=> "station_name='" + s + "'").mkString(" or ") + ")"
        //        whereSql = whereSql + " and (" + item.map(i=> "array_length(array_positions(test_item, '"+ i +"'), 1)>0").mkString(" or ") + ")"
        whereSql
    }

    //gen where sql
    def genTestDetailWhereSQL(product: String, stationName: String) = {
        var whereSql = " where product = '" + product + "'"
        whereSql = whereSql + " and station_name='" + stationName + "'"
        whereSql
    }
}
