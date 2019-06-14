import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.config.ConfigLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CompareResult{

    var configLoader = new ConfigLoader()
    val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

    def main(args: Array[String]): Unit = {
        //val date = "12310107"
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
        /*
        var originalDF = spark.read
            .text("C:\\Users\\foxconn\\Desktop\\RCA\\ask\\new\\WuDang_L7_TLEOL_11_CN94M8702J06MD_1_APR_21_2019_0h_48m_46s.xml")
        originalDF = originalDF.filter(col("value").contains("StepName=\""))
        originalDF = originalDF.selectExpr("split(split(value, 'StepName=\"')[1], '\"')[0] as item", "input_file_name() as filename")
          .selectExpr("trim(item) as oriitem")

        var item = spark.read.option("header", "true")
          .csv("C:\\Users\\foxconn\\Desktop\\item\\item_04220429.csv")
        originalDF.orderBy(desc("oriitem")).show(40, false)
        item.select("item").orderBy(desc("item")).show(40, false)
        item = item.join( originalDF, col("oriitem").equalTo(col("item")), "leftanti")
*/
        var originalDF = spark.read
          .text("C:\\Users\\foxconn\\Desktop\\TaijiBase\\*\\*.xml")
        //item.coalesce(1).write.csv("C:\\Users\\foxconn\\Desktop\\test")
        originalDF = originalDF.filter(col("value").contains("StepName=\"PcaSetPowerUpNVM_WuDang.1")
            .or(col("value").contains("StepName=\"Scan3LEDInfo_SiriusFW"))
            .or(col("value").contains("StepName=\"Scan3GetTargetInfo.1")))
        originalDF.selectExpr("split(split(value, 'StepName=\"')[1], '\"')[0] as item", "input_file_name() as filename")
          .show(100,false)
    }

}
