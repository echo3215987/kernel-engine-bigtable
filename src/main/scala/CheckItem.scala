import java.time.format.DateTimeFormatter
import java.util.Locale

import com.foxconn.iisd.bd.config.ConfigLoader
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CheckItem{

    var configLoader = new ConfigLoader()
    val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

    def main(args: Array[String]): Unit = {
        val date = "01140121"
        val olddate = "02110218"
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
        var df = spark.read
            .text("C:\\Users\\foxconn\\Desktop\\TaijiBase\\TaijiBase_"+date+"\\*.xml")
        df = df.filter(col("value").contains("StepName=\""))
        df = df.selectExpr("split(split(value, 'StepName=\"')[1], '\"')[0] as item", "input_file_name() as filename")
                .selectExpr("trim(item) as item", "filename")
       /* df = df.selectExpr("input_file_name() as filename", "value")
          .groupBy("filename").agg(concat_ws("", collect_list("value")).as("value"))
        df = df.selectExpr("split(value, 'StepName=\"') as split_array")*/
        //df = df.selectExpr("explode(split_array) as item").
        df.show(false)
        var item = spark.read.option("header", "true")
          //.csv("C:\\Users\\foxconn\\Desktop\\item.txt")
            .csv("C:\\Users\\foxconn\\Desktop\\item\\item_"+olddate+".csv")

        //item = item.join(df, col("_c0").equalTo(col("item")),"outer").dropDuplicates("item", "_c0")
        item = item.union(df).dropDuplicates("item")
        item.show(false)
        item.coalesce(1).write.option("header", "true").csv("C:\\Users\\foxconn\\Desktop\\item\\item_"+date)

    }

}
