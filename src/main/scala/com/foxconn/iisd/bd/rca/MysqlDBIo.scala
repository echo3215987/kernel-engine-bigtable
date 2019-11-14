package com.foxconn.iisd.bd.rca

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

@SerialVersionUID(100L)
class MysqlDBIo(configContext: ConfigContext) extends Serializable {

  private var _conn: Connection = null

  private def getMariadbUrl(): String = {
    return configContext.mysqlDbConnUrlStr
  }

  private def getMariadbUser(): String = {
    return configContext.mysqlDbUserName
  }

  private def getMariadbPassword(): String = {
    return configContext.mysqlDbPassword
  }

  def getConn(): Connection = {
    if(_conn == null || _conn.isClosed) {
      _conn = DriverManager.getConnection(
        this.getMariadbUrl(),
        this.getMariadbConnectionProperties())
      _conn.setAutoCommit(false)
    }
    return _conn
  }

  def getNewConn(): Connection = {
    val conn = DriverManager.getConnection(
      this.getMariadbUrl(),
      this.getMariadbConnectionProperties())
    conn.setAutoCommit(false)
    return conn
  }

  def closeConn(): Unit = {
    if(_conn != null && !_conn.isClosed) {
      _conn.close()
      _conn = null
    }
  }

  private def getMariadbConnectionProperties(): Properties ={
    val _mariadbConnectionProperties = new Properties()
    _mariadbConnectionProperties.put("user", this.getMariadbUser())
    _mariadbConnectionProperties.put("password", this.getMariadbPassword())
    return _mariadbConnectionProperties
  }

  def getDfFromMariadb(spark: SparkSession, table: String): DataFrame = {
    spark.read.jdbc(this.getMariadbUrl(), table, this.getMariadbConnectionProperties()).persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def getDfFromMariadbWithQuery(spark: SparkSession, query: String, numPartitions: Int): DataFrame = {
    return spark.read.format("jdbc")
      .option("url", this.getMariadbUrl())
      .option("numPartitions", numPartitions)
      //.option("partitionColumn", primaryKey)
      .option("user", this.getMariadbUser())
      .option("password", this.getMariadbPassword())
      .option("query", query)
      .load()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

//  def saveDB(df: DataFrame, table: String, numExecutors: Int): Unit = {
//    df.write
//      .mode(SaveMode.Append)
//      .option("numPartitions", numExecutors)
//      .jdbc(ConfigContext.mysqlDbConnUrlStr, table, this.getMariadbConnectionProperties())
//  }

  def execSqlToMariadb(sql: String): Unit = {
    val conn = this.getConn()
    conn.createStatement().execute(sql)
    conn.commit()
    conn.close()
  }

  def saveToMariadb(df: DataFrame, table: String, numExecutors: Int): Unit = {

    //INSERT INTO ins_duplicate VALUES (1,'Antelope') ON DUPLICATE KEY UPDATE animal='Antelope';
    val sqlPrefix =
      "REPLACE INTO " + table +
        "(" + df.columns.mkString(",") + ")" +
        " VALUES "

    val batchSize = 2000
    val repartitionSize = numExecutors

    df.rdd.repartition(repartitionSize).foreachPartition{

      partition => {

        val conn = DriverManager.getConnection(this.getMariadbUrl(), this.getMariadbConnectionProperties())

        conn.setAutoCommit(false)

        var count = 0
        var sql = sqlPrefix

        partition.foreach { r =>
          count += 1

          val values = r.mkString("'", "','", "'").replaceAll("'null'", "null")

          sql = sql + "(" + values + ") ,"

          if(count == batchSize){
            conn.createStatement().execute(sql.substring(0, sql.length - 1))
            count = 0
            sql = sqlPrefix

            conn.commit()
          }
        }

        if(count > 0) {
          conn.createStatement().execute(sql.substring(0, sql.length - 1))
        }

        conn.commit()
      }
    }
  }

  def saveToMariadbUpsertSQL(df: DataFrame, table: String, updateIdx: List[Int], numExecutors: Int): Unit = {

    val sqlPrefix =
      "INSERT INTO " + table +
        "(" + df.columns.mkString(",") + ")" +
        " VALUES "

    var updateColumns = updateIdx.map {
      idx => df.columns(idx)
    }
    println(updateColumns)

    val batchSize = 2000
    val repartitionSize = numExecutors

    df.rdd.repartition(repartitionSize).foreachPartition{

      partition => {

        val conn = DriverManager.getConnection(this.getMariadbUrl(), this.getMariadbConnectionProperties())

        conn.setAutoCommit(false)

        var count = 0
        var sql = sqlPrefix

        partition.foreach { r =>
          count += 1

          val values = r.mkString("'", "','", "'").replaceAll("'null'", "null")

          var updateStr = updateIdx.zipWithIndex.map {
            case (columnIdx, idx) => {
              var name = updateColumns(idx)
              //name.concat(" = ").concat(name).concat(" + VALUES('" + r.getString(columnIdx) + "'),")
              name.concat(" = ").concat("'" + r.getString(columnIdx) + "',")
            }
          }.mkString("")
          updateStr = updateStr.substring(0, updateStr.length - 1)

          sql = sql + "(" + values + ") ON DUPLICATE KEY UPDATE " + updateStr + " ,"


          if(count == batchSize){
            //println("寫入Mariadb筆數 : " + count)
            //println("sql : " + sql.substring(0, sql.length - 1))
            conn.createStatement().execute(sql.substring(0, sql.length - 1))
            count = 0
            sql = sqlPrefix

            conn.commit()
          }
        }

        if(count > 0) {
          //println("寫入Mariadb筆數 : " + count)
          //println("sql : " + sql.substring(0, sql.length - 1))
          conn.createStatement().execute(sql.substring(0, sql.length - 1))
        }

        conn.commit()

        conn.close()
      }
    }
  }

  def saveToMariadb(df: DataFrame, table: String, saveMode: SaveMode, numExecutors: Int): Unit = {
    df.write
      .mode(saveMode)
      .option("numPartitions", numExecutors)
      .jdbc(this.getMariadbUrl(), table, this.getMariadbConnectionProperties())
  }

}
