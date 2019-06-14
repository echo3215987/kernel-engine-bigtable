import java.io.FileNotFoundException
import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.config.ConfigLoader
import com.foxconn.iisd.bd.rca.SparkUDF.{parseArrayToString, parseStringToJSONString}
import com.foxconn.iisd.bd.rca.XWJKernelEngine.configLoader
import com.foxconn.iisd.bd.rca.utils.IoUtils
import com.foxconn.iisd.bd.rca.utils.db.MariadbUtils
import org.apache.spark.sql.types.TimestampType
import com.foxconn.iisd.bd.rca.utils.db._
import com.foxconn.iisd.bd.rca.SparkUDF._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{regexp_replace, _}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.Encoders
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.Seq


object Test{

    var configLoader = new ConfigLoader()
    val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

    configLoader.setDefaultConfigPath("""conf/default.yaml""")
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
          .appName("Spark SQL basic example")
          .config("spark.master", "local")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        import spark.implicits._
/*
        var test = Vector("SpecRedNomCh123_Bits8.1=1", "SpecMaxChipTolFromNom_Bits8=2", "SpecMaxChipTolFromNom_Bits7=3")

        var upperValue = test.filter(testparm=> testparm.contains("SpecRedNomCh123_Bits8.1")).mkString.split("=")(1).toFloat
        var upperSub = test.filter(testparm=> testparm.contains("SpecMaxChipTolFromNom_Bits8")).mkString.split("=")(1).toFloat

        println(upperValue)
        println(upperSub)
        println(upperValue-upperSub)

        //var test = Vector("SpecRedNomCh123_Bits8.1=1", "SpecMaxChipTolFromNom_Bits8=2", "SpecMaxChipTolFromNom_Bits7=3")


        var cc = spark.read.option("header", "true").csv("C:\\Users\\foxconn\\Desktop\\cc.csv")
        cc.withColumn("test", getSpec(col("item"), col("compare")))
        def getSpec = udf {
            (item: String, compare: Seq[String]) => item  match {
                case "PcaVerifyFirmwareRev^DReadVersion" => {
                    //var upper = compare.filter(testparm => testparm.contains("ValidFWRevs")).mkString.split("=")(1)
                    var upper = getSpecBoundary(compare, "ValidFWRevs")
                    var lower = upper
                    Vector(upper, lower)
                }
            }

        }

        def getSpecBoundary(compare: Seq[String], spec: String): String = {
            compare.filter(testparm => testparm.contains(spec)).mkString.split("=")(1)
        }
*/

        //try {
        val testDetailColumns = configLoader.getString("log_prop", "test_detail_col")
        val dataSeperator = configLoader.getString("log_prop", "log_seperator")

        val testDetailDestPath = "C:\\Users\\pj17_\\Desktop\\part-00000-884c2f76-b731-4f9c-9ba5-c35e3c1558c3-c000.txt"
        val testDetailSourceDf = IoUtils.getDfFromPath(spark, testDetailDestPath.toString, testDetailColumns, dataSeperator)


            var testDetailTempDf = testDetailSourceDf.distinct()
              .withColumn("test_starttime",
                  unix_timestamp(trim($"test_starttime"),
                      configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
              .withColumn("test_endtime",
                  unix_timestamp(trim($"test_endtime"),
                      configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
              .withColumn("create_time",
                  unix_timestamp(trim($"create_time"),
                      configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
              .withColumn("start_date",
                  unix_timestamp(trim($"start_date"),
                      configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
              .withColumn("update_time",
                  unix_timestamp(trim($"update_time"),
                      configLoader.getString("log_prop", "test_detail_dt_fmt")).cast(TimestampType))
              /*.withColumn("test_item", parseArrayToString(split(trim($"test_item"), "\001")))
              .withColumn("test_item", concat(lit("ARRAY["), $"test_item", lit("]")))
              .withColumn("test_value", parseStringToJSONString(split(trim($"test_value"), "\001")))
              .withColumn("test_upper", parseStringToJSONString(split(trim($"test_upper"), "\001")))
              .withColumn("test_lower", parseStringToJSONString(split(trim($"test_lower"), "\001")))
              .withColumn("test_unit", parseStringToJSONString(split(trim($"test_unit"), "\001")))*/
              .withColumn("test_item", split(trim($"test_item"), "\001"))
              .withColumn("test_value", split(trim($"test_value"), "\001"))
              .withColumn("test_upper", split(trim($"test_upper"), "\001"))
              .withColumn("test_lower", split(trim($"test_lower"), "\001"))
              .withColumn("test_unit", split(trim($"test_unit"), "\001"))
              .withColumn("list_of_failure", regexp_replace($"list_of_failure", "\001", "^A"))
              .withColumn("list_of_failure_detail", regexp_replace($"list_of_failure_detail", "\001", "^A"))
              .persist(StorageLevel.MEMORY_AND_DISK_SER_2)

            testDetailTempDf.select("test_item", "test_value", "test_upper").show(false)
            val testDetailSourceDfDistCnt = testDetailSourceDf.count()



            //TODO: summary file
            //      Summary.setMasterFilesNameList(IoUtils.getFilesNameList(spark, testDetailDestPath))
            //                        testDetailSourceDf.select("test_upper").show(false)
            //                        testDetailSourceDf.select("test_lower").show(false)
            //                        testDetailSourceDf.select("test_unit").show(false)
            //                  testDetailSourceDf.printSchema()
            val testDetailCockroachDf = testDetailTempDf
              .withColumn("test_item", parseArrayToString($"test_item"))
              .withColumn("test_item", concat(lit("ARRAY["), $"test_item", lit("]")))
              .withColumn("test_value", parseStringToJSONString($"test_value"))
              .withColumn("test_upper", parseStringToJSONString($"test_upper"))
              .withColumn("test_lower", parseStringToJSONString($"test_lower"))
              .withColumn("test_unit", parseStringToJSONString($"test_unit"))

            //將資料儲存進Cockroachdb
            println("saveToCockroachdb --> testDetailCockroachDf")
            /*IoUtils.saveToCockroachdb(testDetailCockroachDf,
              configLoader.getString("log_prop", "test_detail_table"),
              numExecutors)
      */

            //insert product station to mysql
            //將測項上下界撈出來之後, 根據測試版號與時間選最新

            testDetailTempDf.select("product", "station_name", "test_item", //"station_id",
                "test_upper", "test_lower", "test_unit", "test_version", "test_starttime")

            testDetailTempDf = testDetailTempDf
              .withColumn("temp", arrays_zip($"test_item", $"test_upper", $"test_lower", $"test_unit"))
              .withColumn("temp", explode($"temp"))//station_id
              .selectExpr("product", "station_name", "temp.test_item as test_item", "temp.test_upper as test_upper",
                "temp.test_lower as test_lower", "temp.test_unit as test_unit", "test_version", "test_starttime")
              //控制字元需要轉換嗎(mysql)
              .withColumn("test_upper", split(split(col("test_upper"), "\004").getItem(1), "\003").getItem(1))
              .withColumn("test_lower", split(split(col("test_lower"), "\004").getItem(1), "\003").getItem(1))
              .withColumn("test_unit", split(split(col("test_unit"), "\004").getItem(1), "\003").getItem(1))

            val productList = testDetailTempDf.select("product").dropDuplicates().as(Encoders.STRING).collect()
            println(productList)

        val df = Seq(
            (1, "TaiJi Base"),
            (2, "Second Value"),
            (3, "TaiJi Base"),
        ).toDF("int_column", "product")

        df.where(col("product").isin(productList:_*)).show(false)

//            val mariadbUtils = new MariadbUtils()
//            val productItemSpecDf = mariadbUtils
//              .getDfFromMariadb(spark, "product_item_spec")
//              .select("product", "station_name", "test_item", //"station_id",
//                  "test_upper", "test_lower", "test_unit", "test_version", "test_starttime")
//              .where(col("product").isin(productList:_*))
//
//
//            productItemSpecDf.show(false)
//
//            val wSpec = Window.partitionBy(col("product"), col("station_name"),
//                col("test_item"))
//              .orderBy(desc("test_version"), desc("test_starttime"))
//
//            testDetailTempDf = testDetailTempDf.withColumn("rank",
//                rank().over(wSpec))
//              .where($"rank".equalTo(1)).drop("rank")
//            testDetailTempDf.show(false)
            //      val updateList = List("test_upper", "test_lower", "test_unit").map(name=> testDetailTempDf.columns.indexOf(name))
            //
            //
            //
            //      //將資料儲存進Mariadb
            //      println("saveToMariadb --> testDetailTempDf")
            //
            //      mariadbUtils.saveToMariadb(
            //        testDetailTempDf,
            //        "product_item_spec",
            //        //updateList,
            //        numExecutors
            //      )


            /*testDetailSourceDf.select("product", "station_name")
                .withColumn("flag", lit(1)),
              "product_station",
              numExecutors*/
            //testDetailTempDf.show(false)
            //testDetailTempDf.printSchema()

            //      testDetailSourceDf.select("product", "station_name", "station_id", "test_version", "test_starttime")
            //        .orderBy($"test_starttime").show(false)

            //testDetailSourceDf.select("product","station_name").withColumn("flag", lit("1"))
//        }catch{
//            case ex: Exception => {
//                // ex.printStackTrace()
//                println("===> Exception !!!")
//            }
//        }

    }

}
