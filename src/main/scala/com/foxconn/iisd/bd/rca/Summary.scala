package com.foxconn.iisd.bd.rca

import java.util

import com.google.gson.Gson
import org.apache.spark.sql.SparkSession

object Summary {

  case class PartMaster(firstTime: String, lastTime: String, totalCnt: Int)
  case class PartDetail(firstTime: String, lastTime: String, totalCnt: Int)
  case class PartTest(firstTime: String, lastTime: String, totalCnt: Int)
  case class RiskAssemblyStationSn(firstTime: String, lastTime: String, totalCnt: Int)
  case class RiskPartSn(firstTime: String, lastTime: String, totalCnt: Int)
  case class RiskTestStationSn(firstTime: String, lastTime: String, totalCnt: Int)

//  case class FilesName(masterFilesNameList: util.ArrayList[String], detailFilesNameList: util.ArrayList[String], testFilesNameList: util.ArrayList[String])
  case class InputFileNames(masterFilesNameList: util.ArrayList[String], detailFilesNameList: util.ArrayList[String], testFilesNameList: util.ArrayList[String])
  case class FilesTotalRows(masterFilesTotalRows: Long, detailFilesTotalRows: Long, testFilesTotalRows: Long)
  case class FilesTotalRowsDist(masterFilesTotalRowsDist: Long, detailFilesTotalRowsDist: Long, testFilesTotalRowsDist: Long)
  case class FirstRowTimeInCockroachdb(masterFirstRowTimeInCockroachdb: String, detailFirstRowTimeInCockroachdb: String, testFirstRowTimeInCockroachdb: String)
  case class LastRowTimeInCockroachdb(masterLastRowTimeInCockroachdb: String, detailLastRowTimeInCockroachdb: String, testLastRowTimeInCockroachdb: String)
  case class TotalRowsInCockroachdb(masterTotalRowsInCockroachdb: Long, detailTotalRowsInCockroachdb: Long, testTotalRowsInCockroachdb: Long)
  case class TotalRowsDistInCockroachdb(masterTotalRowsDistInCockroachdb: Long, detailTotalRowsDistInCockroachdb: Long, testTotalRowsDistInCockroachdb: Long)
  case class FirstRowTimeInMysqldb(masterFirstRowTimeInMysqldb: String, detailFirstRowTimeInMysqldb: String, testFirstRowTimeInMysqldb: String)
  case class LastRowTimeInMysqldb(masterLastRowTimeInMysqldb: String, detailLastRowTimeInMysqldb: String, testLastRowTimeInMysqldb: String)
  case class TotalRowsInMysqldb(masterTotalRowsInMysqldb: Long, detailTotalRowsInMysqldb: Long, testTotalRowsInMysqldb: Long)
  case class Job(jobStartTime: String, jobEndTime: String)
  case class SummaryJson(
                          inputFileNames: InputFileNames,
                          filesTotalRows: FilesTotalRows,
                          filesTotalRowsDist: FilesTotalRowsDist,
                          firstRowTimeInCockroachdb: FirstRowTimeInCockroachdb,
                          lastRowTimeInCockroachdb: LastRowTimeInCockroachdb,
                          totalRowsInCockroachdb: TotalRowsInCockroachdb,
                          totalRowsDistInCockroachdb: TotalRowsDistInCockroachdb,
                          firstRowTimeInMysqldb: FirstRowTimeInMysqldb,
                          lastRowTimeInMysqldb: LastRowTimeInMysqldb,
                          totalRowsInMysqldb: TotalRowsInMysqldb,
                          job: Job)

  var masterFilesRows: util.HashMap[String, Integer] = null
  var detailFilesRows: util.HashMap[String, Integer] = null
  var testFilesRows: util.HashMap[String, Integer] = null

  var masterFilesNameList: util.ArrayList[String] = null
  var detailFilesNameList: util.ArrayList[String] = null
  var testFilesNameList: util.ArrayList[String] = null

  var masterFilesTotalRows: Long = 0
  var detailFilesTotalRows: Long = 0
  var testFilesTotalRows: Long = 0

  var masterFilesTotalRowsDist: Long = 0
  var detailFilesTotalRowsDist: Long = 0
  var testFilesTotalRowsDist: Long = 0

  var masterFirstRowTimeInCockroachdb: String = null
  var detailFirstRowTimeInCockroachdb: String = null
  var testFirstRowTimeInCockroachdb: String = null

  var masterLastRowTimeInCockroachdb: String = null
  var detailLastRowTimeInCockroachdb: String = null
  var testLastRowTimeInCockroachdb: String = null

  var masterTotalRowsInCockroachdb: Long = 0
  var detailTotalRowsInCockroachdb: Long = 0
  var testTotalRowsInCockroachdb: Long = 0

  var masterTotalRowsDistInCockroachdb: Long = 0
  var detailTotalRowsDistInCockroachdb: Long = 0
  var testTotalRowsDistInCockroachdb: Long = 0

  var masterFirstRowTimeInMysqldb: String = null
  var detailFirstRowTimeInMysqldb: String = null
  var testFirstRowTimeInMysqldb: String = null

  var masterLastRowTimeInMysqldb: String = null
  var detailLastRowTimeInMysqldb: String = null
  var testLastRowTimeInMysqldb: String = null

  var masterTotalRowsInMysqldb: Long = 0
  var detailTotalRowsInMysqldb: Long = 0
  var testTotalRowsInMysqldb: Long = 0

  var jobStartTime: String = null
  var jobEndTime: String = null

  def setMasterFilesNameList(masterFilesNameList: util.ArrayList[String]): Unit = {
    this.masterFilesNameList = masterFilesNameList
  }

  def setMasterFilesRows(map: util.HashMap[String, Integer]): Unit = {
    this.masterFilesRows = map;
  }

  def setDetailFilesNameList(detailFilesNameList: util.ArrayList[String]): Unit = {
    this.detailFilesNameList = detailFilesNameList
  }

  def setDetailFilesRows(map: util.HashMap[String, Integer]): Unit = {
    this.detailFilesRows = map;
  }

  def setTestFilesNameList(testFilesNameList: util.ArrayList[String]): Unit = {
    this.testFilesNameList = testFilesNameList
  }

  def setTestFilesRows(map: util.HashMap[String, Integer]): Unit = {
    this.testFilesRows = map;
  }

  def setMasterFilesTotalRows(masterFilesTotalRows: Long): Unit = {
    this.masterFilesTotalRows = masterFilesTotalRows
  }

  def setDetailFilesTotalRows(detailFilesTotalRows: Long): Unit = {
    this.detailFilesTotalRows = detailFilesTotalRows
  }

  def setTestFilesTotalRows(testFilesTotalRows: Long): Unit = {
    this.testFilesTotalRows = testFilesTotalRows
  }

  def setMasterFilesTotalRowsDist(masterFilesTotalRowsDist: Long): Unit = {
    this.masterFilesTotalRowsDist = masterFilesTotalRowsDist
  }

  def setDetailFilesTotalRowsDist(detailFilesTotalRowsDist: Long): Unit = {
    this.detailFilesTotalRowsDist = detailFilesTotalRowsDist
  }

  def setTestFilesTotalRowsDist(testFilesTotalRowsDist: Long): Unit = {
    this.testFilesTotalRowsDist = testFilesTotalRowsDist
  }

  def setMasterFirstRowTimeInCockroachdb(masterFirstRowTimeInCockroachdb: String): Unit = {
    this.masterFirstRowTimeInCockroachdb = masterFirstRowTimeInCockroachdb
  }

  def setDetailFirstRowTimeInCockroachdb(detailFirstRowTimeInCockroachdb: String): Unit = {
    this.detailFirstRowTimeInCockroachdb = detailFirstRowTimeInCockroachdb
  }

  def setTestFirstRowTimeInCockroachdb(testFirstRowTimeInCockroachdb: String): Unit = {
    this.testFirstRowTimeInCockroachdb = testFirstRowTimeInCockroachdb
  }

  def setMasterLastRowTimeInCockroachdb(masterLastRowTimeInCockroachdb: String): Unit = {
    this.masterLastRowTimeInCockroachdb = masterLastRowTimeInCockroachdb
  }

  def setDetailLastRowTimeInCockroachdb(detailLastRowTimeInCockroachdb: String): Unit = {
    this.detailLastRowTimeInCockroachdb = detailLastRowTimeInCockroachdb
  }

  def setTestLastRowTimeInCockroachdb(testLastRowTimeInCockroachdb: String): Unit = {
    this.testLastRowTimeInCockroachdb = testLastRowTimeInCockroachdb
  }

  def setMasterTotalRowsInCockroachdb(masterTotalRowsInCockroachdb: Long): Unit = {
    this.masterTotalRowsInCockroachdb = masterTotalRowsInCockroachdb
  }

  def setDetailTotalRowsInCockroachdb(detailTotalRowsInCockroachdb: Long): Unit = {
    this.detailTotalRowsInCockroachdb = detailTotalRowsInCockroachdb
  }

  def setTestTotalRowsInCockroachdb(testTotalRowsInCockroachdb: Long): Unit = {
    this.testTotalRowsInCockroachdb = testTotalRowsInCockroachdb
  }

  def setMasterTotalRowsDistInCockroachdb(masterTotalRowsDistInCockroachdb: Long): Unit = {
    this.masterTotalRowsDistInCockroachdb = masterTotalRowsDistInCockroachdb
  }

  def setDetailTotalRowsDistInCockroachdb(detailTotalRowsDistInCockroachdb: Long): Unit = {
    this.detailTotalRowsDistInCockroachdb = detailTotalRowsDistInCockroachdb
  }

  def setTestTotalRowsDistInCockroachdb(testTotalRowsDistInCockroachdb: Long): Unit = {
    this.testTotalRowsDistInCockroachdb = testTotalRowsDistInCockroachdb
  }

  def setMasterFirstRowTimeInMysqldb(masterFirstRowTimeInMysqldb: String): Unit = {
    this.masterFirstRowTimeInMysqldb = masterFirstRowTimeInMysqldb
  }

  def setDetailFirstRowTimeInMysqldb(detailFirstRowTimeInMysqldb: String): Unit = {
    this.detailFirstRowTimeInMysqldb = detailFirstRowTimeInMysqldb
  }

  def setTestFirstRowTimeInMysqldb(testFirstRowTimeInMysqldb: String): Unit = {
    this.testFirstRowTimeInMysqldb = testFirstRowTimeInMysqldb
  }

  def setMasterLastRowTimeInMysqldb(masterLastRowTimeInMysqldb: String): Unit = {
    this.masterLastRowTimeInMysqldb = masterLastRowTimeInMysqldb
  }

  def setDetailLastRowTimeInMysqldb(detailLastRowTimeInMysqldb: String): Unit = {
    this.detailLastRowTimeInMysqldb = detailLastRowTimeInMysqldb
  }

  def setTestLastRowTimeInMysqldb(testLastRowTimeInMysqldb: String): Unit = {
    this.testLastRowTimeInMysqldb = testLastRowTimeInMysqldb
  }

  def setMasterTotalRowsInMysqldb(masterTotalRowsInMysqldb: Long): Unit = {
    this.masterTotalRowsInMysqldb = masterTotalRowsInMysqldb
  }

  def getMasterTotalRowsInMysqldb(): Long = {
    masterTotalRowsInMysqldb
  }

  def setDetailTotalRowsInMysqldb(detailTotalRowsInMysqldb: Long): Unit = {
    this.detailTotalRowsInMysqldb = detailTotalRowsInMysqldb
  }

  def getDetailTotalRowsInMysqldb(): Long = {
    detailTotalRowsInMysqldb
  }

  def setTestTotalRowsInMysqldb(testTotalRowsInMysqldb: Long): Unit = {
    this.testTotalRowsInMysqldb = testTotalRowsInMysqldb
  }

  def getTestTotalRowsInMysqldb(): Long = {
    testTotalRowsInMysqldb
  }

  def setJobStartTime(jobStartTime: String): Unit = {
    this.jobStartTime = jobStartTime
  }

  def setJobEndTime(jobEndTime: String): Unit = {
    this.jobEndTime = jobEndTime
  }

  def save(spark: SparkSession): Unit = {
    IoUtils.saveSummaryFileToMinio(spark, getJsonString())
  }

  def getJsonString(): String = {
    val inputFileNames = new InputFileNames(masterFilesNameList, detailFilesNameList, testFilesNameList)
    val filesTotalRows = new FilesTotalRows(masterFilesTotalRows, detailFilesTotalRows, testFilesTotalRows)
    val filesTotalRowsDist = FilesTotalRowsDist(masterFilesTotalRowsDist, detailFilesTotalRowsDist, testFilesTotalRowsDist)
    val firstRowTimeInCockroachdb = new FirstRowTimeInCockroachdb(masterFirstRowTimeInCockroachdb, detailFirstRowTimeInCockroachdb, testFirstRowTimeInCockroachdb)
    val lastRowTimeInCockroachdb = new LastRowTimeInCockroachdb(masterLastRowTimeInCockroachdb, detailLastRowTimeInCockroachdb, testLastRowTimeInCockroachdb)
    val totalRowsInCockroachdb = new TotalRowsInCockroachdb(masterTotalRowsInCockroachdb, detailTotalRowsInCockroachdb, testTotalRowsInCockroachdb)
    val totalRowsDistInCockroachdb = new TotalRowsDistInCockroachdb(masterTotalRowsDistInCockroachdb, detailTotalRowsDistInCockroachdb, testTotalRowsDistInCockroachdb)
    val firstRowTimeInMysqldb = new FirstRowTimeInMysqldb(masterFirstRowTimeInMysqldb, detailFirstRowTimeInMysqldb, testFirstRowTimeInMysqldb)
    val lastRowTimeInMysqldb = new LastRowTimeInMysqldb(masterLastRowTimeInMysqldb, detailLastRowTimeInMysqldb, testLastRowTimeInMysqldb)
    val totalRowsInMysqldb = new TotalRowsInMysqldb(masterTotalRowsInMysqldb, detailTotalRowsInMysqldb, testTotalRowsInMysqldb)
    val job = new Job(jobStartTime, jobEndTime)
    val summaryJson = new SummaryJson(inputFileNames,
                                      filesTotalRows,
                                      filesTotalRowsDist,
                                      firstRowTimeInCockroachdb,
                                      lastRowTimeInCockroachdb,
                                      totalRowsInCockroachdb,
                                      totalRowsDistInCockroachdb,
                                      firstRowTimeInMysqldb,
                                      lastRowTimeInMysqldb,
                                      totalRowsInMysqldb,
                                      job)
    val gson = new Gson
    val jsonString = gson.toJson(summaryJson)
    jsonString
  }

}
