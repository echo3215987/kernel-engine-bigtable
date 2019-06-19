package com.foxconn.iisd.bd.rca

import java.io.FileNotFoundException
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object XWJBigtable {

  var configLoader = new ConfigLoader()
  val datetimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.US)

  def main(args: Array[String]): Unit = {

    val limit = 1
    var count = 0

    println("xwj-bigtable-v1:")

    while (count < limit) {
      println(s"count: $count")

      try {
        configLoader.setDefaultConfigPath("""conf/default.yaml""")
        if (args.length == 1) {
          configLoader.setDefaultConfigPath(args(0))
        }
        XWJBigtable.start()
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      }

      count = count + 1

      Thread.sleep(5000)
    }

  }

  def start(): Unit = {


    var date: java.util.Date = new java.util.Date()
    val flag = date.getTime().toString
    val jobStartTime: String = new SimpleDateFormat(
        configLoader.getString("summary_log_path","job_fmt")).format(date.getTime())
    println("job start time : " + jobStartTime)
    val executeTime: String = new SimpleDateFormat(
      configLoader.getString("log_prop","product_dt_fmt")).format(date.getTime())
    println("execute time : " + executeTime)
//    Summary.setJobStartTime(jobStartTime)

    println(s"flag: $flag" + ": xwj")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkBuilder = SparkSession
      .builder
      .appName(configLoader.getString("spark", "job_name"))
      .master(configLoader.getString("spark", "master"))

    val confStr = configLoader.getString("spark", "conf")

    val confAry = confStr.split(";").map(_.trim)
    for (i <- 0 until confAry.length) {
      val configKeyValue = confAry(i).split("=").map(_.trim)
      println("conf ===> " + configKeyValue(0) + " : " + configKeyValue(1))
      sparkBuilder.config(configKeyValue(0), configKeyValue(1))
    }

    val spark = sparkBuilder.getOrCreate()

    val configMap = spark.conf.getAll
    for ((k, v) <- configMap) {
      println("[" + k + " = " + v + "]")
    }

    configLoader.setConfig2SparkAddFile(spark)

    var logPathSection = "local_log_path"
    val isFromMinio = configLoader.getString("general", "from_minio").toBoolean
    println("isFromMinio : " + isFromMinio)

    if (isFromMinio) {
      logPathSection = "minio_log_path"

      val endpoint = configLoader.getString("minio", "endpoint")
      val accessKey = configLoader.getString("minio", "accessKey")
      val secretKey = configLoader.getString("minio", "secretKey")
      val bucket = configLoader.getString("minio", "bucket")

      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    }
    import spark.implicits._
    val numExecutors = spark.conf.get("spark.executor.instances", "1").toInt

    //val factory = configLoader.getString("general", "factory")

    //val failCondition: Int = configLoader.getString("analysis", "fail_condition").toInt

    //s3a://" + bucket + "/
    val testDetailPath = configLoader.getString(logPathSection, "test_detail_path")

    val testDetailFileLmits = configLoader.getString(logPathSection, "test_detail_file_limits").toInt

    //"sn,build_name,build_description,unit_number,station_id,test_status,test_starttime,test_endtime,list_of_failure,list_of_failure_detail,test_phase,machine_id,factory_code,floor,line_id,test_item,test_value,test_unit,test_lower,test_upper,create_time,update_time,station_name,start_date,product,test_version"
    //CN95I870ZC06MD_||_SOR_||_SOR_||_CN95I870ZC06MD_||_L7_TLEOL_06_||_Exception_||_2019/05/18 06:36_||_2019/05/18 06:36_||_PcaVerifyFirmwareRev_||_Error_||_MP_||__||_CQ_||_D62_||_2_||_ProcPCClockSync^DResultInfo^APcaVerifyFirmwareRev^DResultInfo^APcaVerifyFirmwareRev^DExpectedVersion^APcaVerifyFirmwareRev^DReadVersion^APcaVerifyFirmwareRev^DDateTimeStarted^APcaVerifyFirmwareRev^DActualFWUpdate^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C5/18/2019 5:29:48 AM^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^C^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_ProcPCClockSync^DResultInfo^C^APcaVerifyFirmwareRev^DResultInfo^C^APcaVerifyFirmwareRev^DExpectedVersion^C^APcaVerifyFirmwareRev^DReadVersion^CTJP1FN1845AR^APcaVerifyFirmwareRev^DDateTimeStarted^C^APcaVerifyFirmwareRev^DActualFWUpdate^C169^APcaVerifyFirmwareRev^DFWUpdateDSIDFirst^C_||_2019/05/18 06:36_||_2019/05/18 06:36_||_TLEOL_||_2019/05/18 06:36_||_TaiJi Base_||_42.3.8 REV_37_Taiji25
    val testDetailColumns = configLoader.getString("log_prop", "test_detail_col")

    val dataSeperator = configLoader.getString("log_prop", "log_seperator")

    ///////////
    //載入資料//
    ///////////

    try {

      //先讀dataset setting table
      val mariadbUtils = new MariadbUtils()

      val datasetColumnStr = "id, product, bt_name, bt_create_time, bt_last_time, effective_start_date, effective_end_date"
      val datasetTableStr = "data_set_setting"

      val datasetSql = "select setting.id, setting.product, setting.bt_create_time, " +
        "setting.bt_last_time, setting.bt_next_time, setting.effective_end_date, " +
        "setting.effective_start_date, part.component, item.item, item.station " +
        "from data_set_setting setting, data_set_station_item item, data_set_part part " +
        "where setting.id=item.dss_id and setting.id=part.dss_id " +
        "and setting.effective_start_date>='" + executeTime + "' "+
        "and setting.effective_end_date<='" + executeTime + "' "
      //select id, product, bt_name, bt_create_time, bt_last_time, effective_start_date, effective_end_date
//      val datasetDf = mariadbUtils
//          .execSqlToMariadb("select " + datasetColumnStr + " from " + datasetTableStr
//            + " where effective_start_date<='" + executeTime +"' and effective_end_date>='" + executeTime + "'")
//
        val datasetDf = mariadbUtils
          .execSqlToMariadbToGetResult(spark, datasetSql)


//       val datasetDf = mariadbUtils.getDfFromMariadb(spark, datasetTableStr)
//         .where("effective_start_date<='" + executeTime +"' and effective_end_date>='" + executeTime + "'")
//         .select("id","bt_name", "bt_create_time", "bt_last_time", "effective_start_date", "effective_end_date")
//      datasetDf.show(false)
    } catch {
      case ex: FileNotFoundException => {
        // ex.printStackTrace()
        println("===> FileNotFoundException !!!")
      }
    }
  }

}
