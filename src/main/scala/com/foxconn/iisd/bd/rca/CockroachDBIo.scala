package com.foxconn.iisd.bd.rca

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/*
 *
 *
 * @author JasonLai
 * @date 2019/9/19 下午2:49
 * @description Cockroach Database 存取
 */
@SerialVersionUID(100L)
class CockroachDBIo(configContext: ConfigContext) extends Serializable {

  private def getCockroachdbUrl(): String = {
    return configContext.cockroachDbConnUrlStr
  }

  private def getCockroachdbSSLMode(): String = {
    return configContext.cockroachDbSslMode
  }

  private def getCockroachdbUser(): String = {
    return configContext.cockroachDbUserName
  }

  private def getCockroachdbPassword(): String = {
    return configContext.cockroachDbPassword
  }

  private def getCockroachdbConnectionProperties(): Properties ={

    val _cockroachdbConnectionProperties = new Properties()

    _cockroachdbConnectionProperties.put(
      "user",
      this.getCockroachdbUser()
    )

    _cockroachdbConnectionProperties.put(
      "password",
      this.getCockroachdbPassword()
    )

    _cockroachdbConnectionProperties.put(
      "sslmode",
      this.getCockroachdbSSLMode()
    )
    _cockroachdbConnectionProperties
  }

  def getDfFromCockroachdb(spark: SparkSession, sql: String): DataFrame = {
    spark.read.jdbc(this.getCockroachdbUrl(), "( " + sql + " ) pushdown_query", this.getCockroachdbConnectionProperties()).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  /*
   *
   *
   * @author JasonLai
   * @date 2019/10/2 下午2:55
   * @param [spark, query, numPartitions]
   * @return org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>
   * @description 藉由sql語句 取得 table dataframe
   */
  def getDfFromCockroachdb(spark: SparkSession, query: String, numPartitions: Int): DataFrame = {
    return spark.read.format("jdbc")
      .option("url", this.getCockroachdbUrl())
      .option("numPartitions", numPartitions)
      .option("sslmode", this.getCockroachdbSSLMode())
      .option("user", this.getCockroachdbUser())
      .option("password", this.getCockroachdbPassword())
      .option("query", query)
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def getDfFromCockroachdb(spark: SparkSession, table: String, predicates: Array[String]): DataFrame = {
    spark.read.jdbc(this.getCockroachdbUrl(), table, predicates, this.getCockroachdbConnectionProperties()).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

//  def getDfFromCockroachdb(spark: SparkSession, query: String, numPartitions: Int,
//                           primaryKey: String, lowerBound: String, upperBound: String): DataFrame = {
//    return spark.read.format("jdbc")
//      .option("url", this.getCockroachdbUrl())
//      .option("numPartitions", numPartitions)
//      .option("partitionColumn", primaryKey)
//      .option("lowerBound", lowerBound)
//      .option("upperBound", upperBound)
//      .option("sslmode", this.getCockroachdbSSLMode())
//      .option("user", this.getCockroachdbUser())
//      .option("password", this.getCockroachdbPassword())
//      .option("dbtable", query)
//      .load()
//  }

  def saveToCockroachdb(df: DataFrame, table: String, numExecutors: Int): Unit = {

    val sqlPrefix =
      "UPSERT INTO " + table +
        "(" + df.columns.mkString(",") + ")" +
        " VALUES "

    val batchSize = 3000
    val batchLength = 600000
    val repartitionSize = numExecutors

    df.distinct.rdd.repartition(repartitionSize).foreachPartition{

      partition => {

        val conn = DriverManager.getConnection(this.getCockroachdbUrl(), this.getCockroachdbConnectionProperties())
        conn.setAutoCommit(false)

        var runCount = 0
        var count = 0
        var sql = sqlPrefix

        partition.foreach { r =>
          count += 1

          val values = r.mkString("'", "','", "'")
            .replaceAll("'null'", "null")
            .replaceAll("\"null\"", "null")
            .replaceAll("'ARRAY\\[", "ARRAY[")
            .replaceAll("\\]'", "]")

          sql = sql + "(" + values + ") ,"

          if(sql.length >= batchLength || count == batchSize){
            runCount = runCount + 1

            try {
              println(s"[$runCount]: ${sql.length}")
              conn.createStatement().execute(sql.substring(0, sql.length - 1))
            } catch {
              case e: Exception => {
                println(s"${sql.substring(0, sql.length - 1)}")
                e.printStackTrace()
              }
            }

            count = 0
            sql = sqlPrefix

            conn.commit()

            conn.createStatement().clearWarnings()
          }
        }

        if(count > 0) {
          conn.createStatement().execute(sql.substring(0, sql.length - 1))
        }

        conn.commit()

        conn.close()
      }
    }
  }
}
