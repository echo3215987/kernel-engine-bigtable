import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.config.ConfigLoader
import com.foxconn.iisd.bd.rca.utils.{IoUtils, Summary}
import com.foxconn.iisd.bd.rca.utils.db._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.spark.input.PortableDataStream
import scala.util.Try
import java.nio.charset._

import scala.collection.mutable._

object ParseFile{

    var configLoader = new ConfigLoader()
    val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

    def main(args: Array[String]): Unit = {
        val date = "05130520"
        val spark = SparkSession.builder()
          .appName("Spark SQL basic example")
          .config("spark.master", "local")
          //.config("io.compression.codecs","io.sensesecure.hadoop.xz.XZCodec")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        /*val df = spark.read.textFile("C:\\Users\\foxconn\\Desktop\\vfpa_trans_fail_list_20190513-20190520.tar")
        df.show(false)
        println(df.count())
*/
        var originalDF = spark.read.option("io.compression.codecs","io.sensesecure.hadoop.xz.XZCodec")
            .text("C:\\Users\\foxconn\\Desktop\\WuDang\\WuDang_"+date+"\\*")
            .selectExpr("input_file_name() as filename").distinct()
            .filter(col("filename").contains("06MD")
              .or(col("filename").contains("06PK"))
              .or(col("filename").contains("06PN"))
              .or(col("filename").contains("06P4"))
              .or(col("filename").contains("06PP"))
              .or(col("filename").contains("06PS"))
              .or(col("filename").contains("06PT"))
              .or(col("filename").contains("06PV"))
              .or(col("filename").contains("06PX"))
              .or(col("filename").contains("06PY"))
              .or(col("filename").contains("06PZ"))
              .or(col("filename").contains("06Q2"))
              .or(col("filename").contains("06Q3"))
              .or(col("filename").contains("06Q4"))
              .or(col("filename").contains("06Q7"))
              .or(col("filename").contains("06Q8"))
              .or(col("filename").contains("06Q9"))
              .or(col("filename").contains("06QD"))
              .or(col("filename").contains("070W"))
              .or(col("filename").contains("070X")))
            .filter(not(col("filename").contains("Repair")))
        //.groupBy("filename").agg(concat_ws("", collect_list("value")).as("value"))

        originalDF.show(300, false)
        println(originalDF.count())
        originalDF = originalDF.selectExpr("split(filename, '/')[7] as filename")
          .withColumn("cp", concat(lit("cp "), col("filename"), lit(" C:\\Users\\foxconn\\Desktop\\TaijiBase\\TaijiBase_"+date)))
        originalDF.select("cp").coalesce(1).write.csv("C:\\Users\\foxconn\\Desktop\\WuDang\\WuDang_"+date+"\\test")
    }

}
