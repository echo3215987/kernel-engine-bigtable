import java.net.URI
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties

import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.hadoop.fs._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object DateUtils {
    val DATE_FORMAT = configLoader.getString("log_prop", "product_df_fmt")

    def getDateAsString(d: Date): String = {
        val dateFormat = new SimpleDateFormat(DATE_FORMAT)
        dateFormat.format(d)
    }

    def convertStringToDate(s: String): Date = {
        val dateFormat = new SimpleDateFormat(DATE_FORMAT)
        dateFormat.parse(s)
    }

}
