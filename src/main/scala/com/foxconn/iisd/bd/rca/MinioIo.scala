package com.foxconn.iisd.bd.rca

import java.io.{BufferedInputStream, FileNotFoundException, IOException, PrintWriter}
import java.net.URI
import java.text.DecimalFormat
import java.util

import org.apache.hadoop.fs._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.io.Source

class MinioIo(configContext: ConfigContext) extends Serializable {

  var job = new Job()

  /*
   *  
   * 
   * @author EchoLee
   * @date 2019/11/7 上午10:38
   * @param [spark, srcPathStr, fileLimits, yearMonth, day, hourMinuteSecond]
   * @return org.apache.hadoop.fs.Path
   * @description 攤平資料夾階層，判斷檔案限制大小，進行資料搬移，並回傳目的地路徑
   */
  def flatMinioFiles(spark: SparkSession, srcPathStr: String, fileLimits: Long,
                     yearMonth: String, day: String, hourMinuteSecond: String): Path = {
    var count = 1
    var totalSize: Long = 0

    val fileSystem = FileSystem.get(URI.create(srcPathStr), spark.sparkContext.hadoopConfiguration)


    println(s"======> srcPathStr : ${srcPathStr}")

    val srcPath = new Path(srcPathStr)
    val destPath =
      new Path(
        new Path(
          new Path(
            new Path(srcPath.getParent, s"${srcPath.getName}_TMP"),
            s"${yearMonth}"),
          s"${day}"),
        s"${hourMinuteSecond}")

    if(!fileSystem.exists(destPath)){
      fileSystem.mkdirs(destPath)
    }

    //summaryfile
    var filesNameList = new util.ArrayList[String]

    try {
      val pathFiles = fileSystem.listFiles(srcPath, true)
      println("======> fileLimits MB : " + getNetFileSizeDescription(fileLimits))
      while (totalSize <= fileLimits && pathFiles.hasNext()) {
        val file = pathFiles.next()
        val filename = file.getPath.getName
        val tmpFilePath = new Path(destPath, filename)
        println(s"======> file.getLen : ${file.getLen}")
        if (file.getLen > 0) {
          println("======> number : " + count +
            " , [MOVE] " + file.getPath + " -> " + tmpFilePath.toString +
            " , file size : " + getNetFileSizeDescription(file.getLen))
          FileUtil.copy(fileSystem, file.getPath, fileSystem, tmpFilePath, false, true, spark.sparkContext.hadoopConfiguration)
//          fileSystem.rename(file.getPath, tmpFilePath)
          count = count + 1
          totalSize = totalSize + file.getLen
          println("======> totalSize add getLen : " + getNetFileSizeDescription(totalSize))

          //add file path
          filesNameList.add(file.getPath.toString)

          Thread.sleep(100)
        }
      }
//      configContext.residualCapacityDataSize = fileLimits - totalSize
      println("======> files total size : " + getNetFileSizeDescription(totalSize))
//      println("======> residual capacity data size : " + getNetFileSizeDescription(configContext.residualCapacityDataSize))
    } catch {
      case ex: FileNotFoundException => {
        println("===> FileNotFoundException !!!")
      }
      case ex: IOException => {
        println("===> IOException !!!")
      }
    }

    if(destPath.toString.indexOf(XWJBigtableConstants.MINIO_TEST_DETAIL_PATH) > 0) {
      SummaryFile.testDetailFilesNameList = filesNameList
    } else if(destPath.toString.indexOf(XWJBigtableConstants.MINIO_WO_PATH) > 0) {
      SummaryFile.woFilesNameList = filesNameList
    } else if(destPath.toString.indexOf(XWJBigtableConstants.MINIO_MAT_PATH) > 0) {
      SummaryFile.matFilesNameList = filesNameList
    } else {
      // error
      println(s"======> Path that does not exist !!!")
      throw new RuntimeException("Path that does not exist")
    }
    destPath
  }

  /*
   *  
   * 
   * @author EchoLee
   * @date 2019/11/7 上午10:27
   * @param [spark, path, columns, dataSeperator]
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 將目的路徑底下的所有檔案資料建立為spark dataframe
   */
  def getDfFromPath(spark: SparkSession, path: String, columns: String, dataSeperator: String): DataFrame = {
    val schema = StructType(columns
      .split(",")
      .map(fieldName => StructField(fieldName, StringType, true)))

    val rdd = spark
      .sparkContext
      .textFile(path)
      .map(_.replace("'", "、"))
      .map(_.split(dataSeperator, schema.fields.length).map(field => {
        if(field.isEmpty)
          ""
        else if(field.contains(XWJBigtableConstants.ctrlCCode))//控制字元不濾掉空白
          field
        else
          field.trim
      }))
      .map(p => Row(p: _*))

//    rdd.take(10).map(println)

    spark.createDataFrame(rdd, schema).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/1 上午11:14
   * @param [size]
   * @return java.lang.String
   * @description
   */
  def getNetFileSizeDescription(size :Long): String = {
    val bytes = new StringBuffer()
    val format = new DecimalFormat("###.0")
    if (size >= 1024 * 1024 * 1024) {
      val i = (size / (1024.0 * 1024.0 * 1024.0))
      bytes.append(format.format(i)).append("GB")
    }
    else if (size >= 1024 * 1024) {
      val i = (size / (1024.0 * 1024.0));
      bytes.append(format.format(i)).append("MB")
    }
    else if (size >= 1024) {
      val i = (size / (1024.0));
      bytes.append(format.format(i)).append("KB")
    }
    else if (size < 1024) {
      if (size <= 0) {
        bytes.append("0B")
      }
      else {
        bytes.append(size.toInt).append("B")
      }
    }
    bytes.toString()
  }

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/1 上午11:14
   * @param [spark, summaryJsonStr]
   * @return void
   * @description
   */
  def saveSummaryFileToMinio(spark: SparkSession,
                             summaryJsonStr: String): Unit = {

    val outputPathStr = configContext.summaryFileLogBasePath
    val tag = configContext.summaryFileLogTag
    val fileExtension = configContext.summaryFileExtension
    val bucket = configContext.minioBucket

    val outputPath = new Path(outputPathStr)
    val fileSystem = FileSystem.get(URI.create(outputPath.getParent.toString), spark.sparkContext.hadoopConfiguration)
    if(!fileSystem.exists(outputPath)){
      fileSystem.mkdirs(outputPath)
    }

    import java.time.LocalDate
    val yearStr: String = LocalDate.now.getYear.toString
    val day = LocalDate.now.getDayOfMonth
    val month = LocalDate.now.getMonthValue
    var monthStr: String = ""
    if(month < 10) {
      monthStr = "0" + month.toString
    } else {
      monthStr = month.toString
    }
    var dayStr: String = ""
    if(day < 10) {
      dayStr = "0" + day.toString
    } else {
      dayStr = day.toString
    }
    val srcPath = new Path(outputPathStr)
    val destPath = new Path(new Path(srcPath, s"${tag}"), s"${yearStr}"+s"${monthStr}")
    if(!fileSystem.exists(destPath)){
      fileSystem.mkdirs(destPath)
    }
    var fileInput: FSDataInputStream = null
    var builder: StringBuffer = new StringBuffer()
    val outputFileName = destPath.toString + "/" + bucket.toUpperCase() + "_" + tag + "_" + yearStr + monthStr + dayStr + "." + fileExtension
    println(s"======> summary file output file name : $outputFileName")
    var output: FSDataOutputStream = null
    if(fileSystem.exists(new Path(outputFileName))) {
      fileInput = fileSystem.open(new Path(destPath.toString + "/" + bucket.toUpperCase() + "_" + tag + "_" + yearStr + monthStr + dayStr + "." + fileExtension))
      Source.fromInputStream(new BufferedInputStream(fileInput)).getLines().foreach { line => builder.append(line.toString + "\n") }
    }
    output = fileSystem.create(new Path(outputFileName))
    val writer = new PrintWriter(output)
    try {
      if(fileInput != null) {
        writer.write(builder.toString())
      }
      writer.write(summaryJsonStr)
    } catch {
      case ex: Exception => {
        println(s"======> ERROR: $ex")
      }
    }
    finally {
      if(writer != null) {
        writer.flush()
        writer.close()
      }
    }
  }

  def moveFilesByJobStatus(): Unit = {

    this.job = configContext.job
    val isJobStatus = configContext.isJobState
    val testDetailPath = configContext.testDetailPath
    val woPath = configContext.woPath
    val matPath = configContext.matPath
    val jobId = job.jobId
    val yearMonth = getJobIdTime(0, 4).toString + getJobIdTime(4, 6).toString
    val day = getJobIdTime(6, 8).toString
    val hourMinuteSecond = getJobIdTime(8, 10).toString + getJobIdTime(10, 12).toString + getJobIdTime(12, 14).toString

    println(s"======> moveFilesByJobStatus")
    try {
      var status = ""
      if(isJobStatus) {
        println(XWJBigtableConstants.JOB_SUCCEEDED)
        status = "Succeeded"
      } else {
        println(XWJBigtableConstants.JOB_FAILED)
        status = "Failed"
      }
      val fileSystem = FileSystem.get(URI.create(testDetailPath), configContext.sparkSession.sparkContext.hadoopConfiguration)
      moveFiles(testDetailPath, fileSystem, yearMonth, day, hourMinuteSecond, jobId, status)
      moveFiles(woPath, fileSystem, yearMonth, day, hourMinuteSecond, jobId, status)
      moveFiles(matPath, fileSystem, yearMonth, day, hourMinuteSecond, jobId, status)
    } catch {
      case ex: Exception => {
        println("======> Exception !!!")
      }
    }
  }

  def moveFiles(srcFilePath: String, fileSystem: FileSystem,
                yearMonth: String, day: String, hourMinuteSecond: String,
                jobId: String, status: String): Unit = {
    try {
      val srcPath = new Path(srcFilePath)
      val srcTmpPath =
        new Path(
          new Path(
            new Path(
              new Path(srcPath.getParent, s"${srcPath.getName}_TMP"),
              s"${yearMonth}"),
            s"${day}"),
          s"${hourMinuteSecond}")
      val destPath =
        new Path(
          new Path(
            new Path(
              new Path(srcPath.getParent, s"${srcPath.getName}_${status}"),
              s"${yearMonth}"),
            s"${day}"),
          s"${jobId}")
      if(!fileSystem.exists(destPath)){
        fileSystem.mkdirs(destPath)
      }
      val pathFiles = fileSystem.listFiles(srcTmpPath, true)
      var count = 1
      while (pathFiles.hasNext()) {
        val file = pathFiles.next()
        val filename = file.getPath.getName
        val destFilePath = new Path(destPath, filename)
        if (file.getLen > 0) {
          println("======> number : " + count + " , [MOVE] " + file.getPath + " -> " + destFilePath.toString)
          fileSystem.rename(file.getPath, destFilePath)
          Thread.sleep(2000)
        }
        count = count + 1
      }
      // no file situation
      if(count == 1) {
        fileSystem.delete(destPath, true)
      }
      fileSystem.delete(srcTmpPath, true)
    } catch {
      case ex: Exception => {
        println("======> Exception !!!")
      }
    }
  }

  def getJobIdTime(startIndex: Int, endIndex: Int): String = {
    this.job.jobId.split("-uuid-")(1).split("-")(0).slice(startIndex, endIndex)
  }
}
