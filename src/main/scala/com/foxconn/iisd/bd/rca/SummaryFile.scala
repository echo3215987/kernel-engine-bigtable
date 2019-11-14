package com.foxconn.iisd.bd.rca

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.google.gson.Gson
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._

object SummaryFile {
//  case class DatasetList(datasetList: util.ArrayList[Dataset])
  case class Dataset(product: String, data_set_id: String, data_set_item_columns: Int,
                     sn_count: Long, start_time: String, end_time: String, spend_time: String)
  case class Job(id: String, startTime: String, endTime: String, status: String, message: String)
  case class XWJBigtableSummaryJson(template_json: String, xwjKEBigtable: util.ArrayList[SummaryJson])
  case class SummaryJson(datasetList: util.ArrayList[Dataset],
                          job: Job,
                          bu: String)

  var testDetailFilesNameList: util.ArrayList[String] = null
  var woFilesNameList: util.ArrayList[String] = null
  var matFilesNameList: util.ArrayList[String] = null

  var startTime: String = null
  var endTime: String = null
  var id: String = null
  var status: String = ""
  var message: String = ""

  var datasetList = List[Dataset]()

  var summaryFileJobFmt = ""
  var summaryFileBuName = ""

  def save(configContext: ConfigContext): Unit = {

    summaryFileJobFmt = configContext.summaryFileJobFmt
    summaryFileBuName = configContext.summaryFileBuName

    //get job
    this.id = configContext.job.jobId
    this.startTime = ((new SimpleDateFormat(configContext.summaryFileJobFmt)).format(configContext.job.jobStartTime))
    this.endTime = ((new SimpleDateFormat(configContext.summaryFileJobFmt)).format(configContext.job.jobEndTime))
    configContext.isJobState match {
      case true =>
        this.status = XWJBigtableConstants.SUMMARYFILE_SUCCEEDED
      case false =>
        this.status = XWJBigtableConstants.SUMMARYFILE_FAILED
      case _ => new IllegalArgumentException
    }
    this.message = ""
    //get dataset
    this.datasetList = configContext.datasetList

    val minioIo = new MinioIo(configContext)
    minioIo.saveSummaryFileToMinio(configContext.sparkSession, getJsonString())
  }

  def getJsonString(): String = {
    val job = new Job(id, startTime, endTime, status, message)
    val summaryJson = new SummaryJson(new java.util.ArrayList[Dataset](datasetList.asJava), job, summaryFileBuName)
    val summaryJsonList = new util.ArrayList[SummaryJson]()
    summaryJsonList.add(summaryJson)
    val xwjBigtableSummaryJson = new XWJBigtableSummaryJson("rca-xwj-ke-bigtable", summaryJsonList)
    val gson = new Gson
    val jsonString = gson.toJson(xwjBigtableSummaryJson)

    jsonString
  }

  def getStartTime(df: DataFrame): String = {
    val startTimeList = df
      .select("start_time")
      .limit(1)
      .rdd
      .map(r => r(0))
      .collect
      .toList
      .asInstanceOf[List[Timestamp]]

    (new SimpleDateFormat(summaryFileJobFmt)).format(startTimeList(0))
  }

  def getTotalRows(df: DataFrame): Long = {
    val totalRowsList = df
      .select("cnt")
      .limit(1)
      .rdd
      .map(r => r(0))
      .collect
      .toList
      .asInstanceOf[List[Long]]

    totalRowsList(0)
  }
}
