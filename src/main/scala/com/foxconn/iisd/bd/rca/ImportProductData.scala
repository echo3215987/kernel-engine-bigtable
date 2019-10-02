package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.net.InetAddress
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.{Date, Locale, Properties}

import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{unix_timestamp, _}
import org.apache.spark.sql.types.{TimestampType, StringType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


object ImportProductData {

  def main(args: Array[String]): Unit = {

//      val productionUrl = "jdbc:mysql:loadbalance://10.57.232.61:3308/rca-cartridge-nesta?useSSL=false&serverTimezone=Asia/Taipei&useUnicode=true&characterEncoding=UTF-8"
      val productionUrl = "jdbc:mysql:loadbalance://10.134.224.194:3306/rca-cartridge-nesta?useSSL=false&serverTimezone=Asia/Taipei&useUnicode=true&characterEncoding=UTF-8"
      val sparkBuilder = SparkSession
        .builder
        .appName("ImportProductData")
        .master("local")
      val spark = sparkBuilder.getOrCreate()
      var df = spark.read.option("header", "true").csv("C:\\Users\\foxconn\\Desktop\\nesta_0924.csv")
//      df.groupBy(col("product")).count().show(210, false)
//      df.groupBy(col("product")).count().filter(col("count").geq(2)).show(false)
      df = df.withColumn("product", trim(col("product")))
      df.groupBy(col("product")).count().filter(col("count").geq(2)).show(false)
      df = df.distinct()
      println(df.count())
      df = df.withColumn("customer", lit("HP"))
        .withColumn("job_start_time", lit("2019-09-26"))
        .withColumn("job_end_time", lit("2020-12-31"))
        .withColumn("true_fail_rule", lit(1))
        .withColumn("md_tolerate_time", lit(0))
        .withColumn("ta_tolerate_time", lit(0))
        .withColumn("upload_freq", lit(0))
        .withColumn("verify_start_time", lit("2019-09-26"))
        .withColumn("verify_end_time", lit("2019-09-26 23:59:59"))
        .withColumn("is_enable", lit(1))
        .withColumn("create_user", lit("admin"))
        .withColumn("create_time", lit("2019-09-27 14:30:00"))
        .withColumn("modify_user", lit("admin"))
        .withColumn("modify_time", lit("2019-09-27 14:30:00"))


// CN047-30008
//|N9H78-30001
//|CN045-30006
//|CN046-30006


      val mariadbConnectionProperties = new Properties()

      mariadbConnectionProperties.put(
        "user", "edgeserver")

      mariadbConnectionProperties.put(
        "password", "Foxconn123654!@"
      )

    df.show()
    println(df.count())


    df.write
      .mode(SaveMode.Append)
      .jdbc(productionUrl, "product_info", mariadbConnectionProperties)


      var oldShift = spark.read.format("jdbc")
        .option("url", productionUrl)
        .option("user", "edgeserver")
        .option("password", "Foxconn123654!@")
        .option("query", "select * from shift")
        .load()

    oldShift = oldShift.withColumn("product", trim(col("product").cast(StringType)))
      .withColumn("start_time", regexp_replace(trim(col("start_time").cast(StringType)), "1970-01-01 ", ""))
      .withColumn("stop_time", regexp_replace(trim(col("stop_time").cast(StringType)), "1970-01-01 ", ""))

    oldShift.show(false)

    var shiftMorDF = df.select("product")
    shiftMorDF = shiftMorDF
      .withColumn("start_time", regexp_replace(lit("08:00:00").cast(StringType), "1970-01-01 ", ""))
      .withColumn("stop_time", regexp_replace(lit("20:00:00").cast(StringType), "1970-01-01 ", ""))
      .withColumn("update_time", lit("2019-09-27 14:30:00"))
      .withColumn("description", lit("morning"))
      .join(oldShift, Seq("product", "start_time", "stop_time"), "left_anti")

    println(shiftMorDF.count())

    shiftMorDF.write
      .mode(SaveMode.Append)
      .jdbc(productionUrl, "shift", mariadbConnectionProperties)


    var shiftNigDF = df.select("product")
    shiftNigDF = shiftNigDF
      .withColumn("start_time", regexp_replace(lit("20:00:00").cast(StringType), "1970-01-01 ", ""))
      .withColumn("stop_time", regexp_replace(lit("08:00:00").cast(StringType), "1970-01-01 ", ""))
      .withColumn("update_time", lit("2019-09-27 14:30:00"))
      .withColumn("description", lit("night"))
      //      .union(oldShift)
      //      .dropDuplicates(Seq("product", "start_time", "stop_time"))
      .join(oldShift, Seq("product", "start_time", "stop_time"), "left_anti")

    println(shiftNigDF.count())

    shiftNigDF.write
      .mode(SaveMode.Append)
      .jdbc(productionUrl, "shift", mariadbConnectionProperties)


  }
}
