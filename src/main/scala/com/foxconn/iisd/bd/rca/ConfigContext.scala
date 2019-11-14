package com.foxconn.iisd.bd.rca

import com.foxconn.iisd.bd.rca.XWJBigtable.{configContext, configLoader}
import com.foxconn.iisd.bd.rca.MysqlDBIo
import com.foxconn.iisd.bd.rca.SummaryFile.Dataset
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
 *
 *
 * @author JasonLai
 * @date 2019/10/3 下午3:49
 * @description 程式所需的所有設定值
 */
class ConfigContext extends Serializable {

  //env
  var env = ""
  //flag
  var flag = ""
  //job
  var job: Job = null
  var jobDateFmt = ""
  var isJobState = false
  // data processing
  var dataProcessingMode = ""
  //file
  var testDetailPath = ""
  var woPath = ""
  var matPath = ""
  var testDetailColumns = ""
  var woColumns = ""
  var matColumns = ""
  var testDetailDtFmt = ""
  var woDtFmt = ""
  var matDtFmt = ""
  var dataSeperator = ""
  var mbLimits = 0
  var residualCapacityDataSize: Long = 0
  //product
  var productDtFmt = ""
  var datasetSettingColumns = ""
  var datasetDatatypeColumns = ""
  var itemInfo = ""
  var stationInfo = ""
  var componentInfo = ""
  var stationInfoColumns = ""
  var itemInfoColumns = ""
  var componentInfoColumns = ""
  var datasetPath = ""
  //bigtable
  var testDetailFilterColumns = ""
  var detailFilterColumns = ""
  var detailColumns = ""
  var stationName = ""
  var item = ""
  var component = ""
  var numPartitions: Int = 0
  var datasetList: List[Dataset] = null

  //spark
  var sparkJobName = ""
  var sparkMaster = ""
  var sparkSession: SparkSession = null
  var sparkNumExcutors = 1
  //minio
  var minioEndpoint = ""
  var minioAccessKey = ""
  var minioSecretKey = ""
  var minioBucket = ""
  var minioConnectionSslEnabled = false
  //bu
  var factory = ""
  //cockroachdb
  var cockroachDbConnUrlStr = ""
  var cockroachDbSslMode = ""
  var cockroachDbUserName = ""
  var cockroachDbPassword = ""
  var cockroachDbMasterTable = ""
  var cockroachDbDetailTable = ""
  var cockroachDbTestTable = ""
  var cockroachDbTestDetailTable = ""
  var cockroachDbWoTable = ""
  var cockroachDbMatTable = ""
  var cockroachDbMasterColumns = ""
  var cockroachDbDetailColumns = ""

  //mysqldb
  var mysqlDbConnUrlStr = ""
  var mysqlDatabase = ""
  var mysqlDbUserName = ""
  var mysqlDbPassword = ""
  var mysqlProductItemSpecTable = ""
  var mysqlProductStationTable = ""
  var mysqlProductMatTable = ""
  var mysqlProductBigtableDatatypeTable = ""
  //micro batch time group
  var microBatchTimeGroupDurationMin = 10
  //datafrmae
  var productBwListTableDf: DataFrame = null
  var productInfoTableDf: DataFrame = null
  //datasource
  var fileDataSource: FileSource = null
  var cockroachDBIo: CockroachDBIo = null
  var mysqlDBIo: MysqlDBIo = null
  //summaryfile
  var summaryFileLogBasePath = ""
  var summaryFileLogTag = ""
  var summaryFileExtension = ""
  var summaryFileJobFmt = ""
  var summaryFileBuName = ""
  //daily
  var dailyCalculationStartHour = 0
  var dailyCalculationDurationHour = 0
  //record runtime
  var processingStartTime: Long = _
  var processingEndTime: Long = _

}
