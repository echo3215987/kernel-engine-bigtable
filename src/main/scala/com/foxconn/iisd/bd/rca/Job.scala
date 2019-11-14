package com.foxconn.iisd.bd.rca

import java.net.InetAddress
import java.text.SimpleDateFormat
import java.util.Date

class Job extends Serializable {

  var jobId = ""

  var jobStartTime: Date = null

  var jobEndTime: Date = null

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/1 上午10:57
   * @param []
   * @return void
   * @description
   */
  def setJobId(configContext: ConfigContext) = {
    //job id sameple : rca-ke-dev-uuid-20190919101500-driver

    configContext.env match {
      case XWJBigtableConstants.ENV_LOCAL =>
        println(s"======> env : local")
        val date = new Date()
        val sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        jobId = "rca-ke-dev-uuid-" + sdf.format(date) + "-driver"
      case XWJBigtableConstants.ENV_DEV =>
        println(s"======> env : dev")
        jobId = getHostName()
      case XWJBigtableConstants.ENV_PROD =>
        println(s"======> env : prod")
        jobId = getHostName()
      case _  => "======> Env Error !!!!!"
    }
  }

  /*
   *  
   * 
   * @author JasonLai
   * @date 2019/10/1 上午10:58
   * @param []
   * @return java.lang.String
   * @description
   */
  def getHostName(): String = {
    var hostName = ""
    try {
      val ip = InetAddress.getLocalHost()
      hostName = ip.getHostName
    } catch {
      case ex: Exception => {
        println("===> Get Pod Hostname Exception !!!")
        ex.printStackTrace()
      }
    }
    hostName
  }

}
