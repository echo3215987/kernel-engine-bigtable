package com.foxconn.iisd.bd.test.rca

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.foxconn.iisd.bd.test.rca.KernelEngineTest.configLoader

class MariadbUtilsTest {

    private var _conn: Connection = null

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

    private def getMariadbUrl(): String = {
        return configLoader.getString("mariadb", "conn_str")
    }

    private def getMariadbConnectionProperties(): Properties ={
        val _mariadbConnectionProperties = new Properties()

        _mariadbConnectionProperties.put(
            "user",
            configLoader.getString("mariadb", "username")
        )

        _mariadbConnectionProperties.put(
            "password",
            configLoader.getString("mariadb", "password")
        )

        return _mariadbConnectionProperties
    }

    def getDfFromMariadb(spark: SparkSession, table: String): DataFrame = {
        return spark.read.jdbc(this.getMariadbUrl(), table, this.getMariadbConnectionProperties())
    }

    def execSqlToMariadb(sql: String): Unit = {
        val conn = this.getConn()

        conn.createStatement().execute(sql)

        conn.commit()
    }

    def saveToMariadb(df: DataFrame, table: String, numExecutors: Int): Unit = {
        val mariadbUrl = configLoader.getString("mariadb", "conn_str")
        val mariadbConnectionProperties = new Properties()

        mariadbConnectionProperties.put(
            "user",
            configLoader.getString("mariadb", "username")
        )

        mariadbConnectionProperties.put(
            "password",
            configLoader.getString("mariadb", "password")
        )

        //INSERT INTO ins_duplicate VALUES (1,'Antelope') ON DUPLICATE KEY UPDATE animal='Antelope';
        val sqlPrefix =
            "REPLACE INTO " + table +
                "(" + df.columns.mkString(",") + ")" +
                " VALUES "

        val batchSize = 2000
        val repartitionSize = numExecutors

        df.rdd.repartition(repartitionSize).foreachPartition{

            partition => {

                val conn = DriverManager.getConnection(
                    mariadbUrl,
                    mariadbConnectionProperties)

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
//    def saveToMariadb(df: DataFrame, table: String): Unit = {
//
//        //INSERT INTO ins_duplicate VALUES (1,'Antelope') ON DUPLICATE KEY UPDATE animal='Antelope';
//        val sqlPrefix =
//            "INSERT INTO " + table +
//                "(" + df.columns.mkString(",") + ")" +
//                " VALUES "
//
//        val batchSize = 2000
//        val repartitionSize = (df.count()/batchSize).toInt + 1
//
//        df.rdd.repartition(repartitionSize).foreachPartition{
//
//            partition => {
//
//                val conn = this.getNewConn()
//                var count = 0
//                var sql = sqlPrefix
//
//                partition.foreach { r =>
//                    count += 1
//
//                    val values = r.mkString("'", "','", "'").replaceAll("'null'", "null")
//
//                    sql = sql + "(" + values + ") ,"
//
//                    if(count == batchSize){
//                        conn.createStatement().execute(sql.substring(0, sql.length - 1))
//                        count = 0
//                        sql = sqlPrefix
//
//                        conn.commit()
//                    }
//                }
//
//                if(count > 0) {
//                    conn.createStatement().execute(sql.substring(0, sql.length - 1))
//                }
//
//                conn.commit()
//            }
//        }
//    }
}
