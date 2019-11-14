package com.foxconn.iisd.bd.rca

import org.apache.spark.sql.DataFrame

/*
 *
 *
 * @author EchoLee
 * @date 2019/11/7 上午9:47
 * @param
 * @return
 * @description 針對檔案形式來源的資料進行存取操作
 */
@SerialVersionUID(114L)
class FileSource(configContext: ConfigContext) extends BaseDataSource with Serializable {

  var testDetailPath = ""
  var woPath = ""
  var matPath = ""
  var testDetailColumns = ""
  var woColumns = ""
  var matColumns = ""
  var dataSeperator = ""
  var mbLimits = 0
  var job = new Job()
//  val minioIo = new MinioIo(configContext)

  /*
   *
   *
   * @author EchoLee
   * @date 2019/11/7 上午9:39
   * @description 初始化File所需的參數
   */
  override def init(): Unit = {
    testDetailPath = configContext.testDetailPath
    woPath = configContext.woPath
    matPath = configContext.matPath
    testDetailColumns = configContext.testDetailColumns
    woColumns = configContext.woColumns
    matColumns = configContext.matColumns
    dataSeperator = configContext.dataSeperator
    mbLimits = configContext.mbLimits
    //use minio service
    setMinioS3()
    job = configContext.job
  }

  /*
  *
  *
  * @author EchoLee
  * @date 2019/11/7 上午9:46
  * @param []
  * @return void
  * @description sparkSession 設定minio s3參數
  */
  def setMinioS3(): Unit = {
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", configContext.minioEndpoint)
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", configContext.minioConnectionSslEnabled.toString)
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", configContext.minioAccessKey)
    configContext.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", configContext.minioSecretKey)
  }
}
