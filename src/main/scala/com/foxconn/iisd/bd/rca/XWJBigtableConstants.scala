package com.foxconn.iisd.bd.rca

/*
 *
 *
 * @author JasonLai
 * @date 2019/9/19 上午9:52
 * @param
 * @return
 * @description Kernel Engine 程式中所有定值
 */
object XWJBigtableConstants {

  //job
  val JOB_SUCCEEDED = "@Job_Result_200"
  val JOB_FAILED = "@Job_Result_300"
  val JOB_EMPTY_DATA = "@Job_Result_301"

  //MINIO FILE PATH
  val MINIO_TEST_DETAIL_PATH = "TEST_DETAIL"
  val MINIO_WO_PATH = "WO"
  val MINIO_MAT_PATH = "MAT"

  //env
  val ENV_LOCAL = "local"
  val ENV_DEV = "dev"
  val ENV_PROD = "prod"

  //summaryfile
  val SUMMARYFILE_SUCCEEDED = "SUCCEEDED"
  val SUMMARYFILE_WARRING = "WARRING"
  val SUMMARYFILE_FAILED = "FAILED"

  //column split code
  val ctrlACode = "\001"
  val ctrlAValue = "^A"
  val ctrlCCode = "\003"
  val ctrlDCode = "\004"
  val ctrlDValue = "^D"

  //mb
  val mb = 1024 * 1024
}
