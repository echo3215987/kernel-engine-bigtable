package com.foxconn.iisd.bd.rca

import java.sql.{Connection, DriverManager, ResultSet}
import java.util.Properties

import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.rdd.JdbcRDD

class MariadbUtils {

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

    private def getMariadbUser(): String = {
        return configLoader.getString("mariadb", "username")
    }

    private def getMariadbPassword(): String = {
        return configLoader.getString("mariadb", "password")
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

        val rs = conn.createStatement().execute(sql)

        conn.commit()
    }

    def getRowFromResultSet(resultSet: ResultSet, colCount: Int): Row ={
        var i:Int = 1
//        var rowStr = ""
        var seq : Seq[String] = Seq()
        while(i <= colCount){
//            rowStr = rowStr+resultSet.getString(i) + ","

            //println(resultSet.getString(i))
            seq = seq :+ resultSet.getString(i)

            //println(seq)
            i += 1
        }
        Row.fromSeq(seq)

//        rowStr
    }

    def execSqlToMariadbToGetResult(spark: SparkSession, sql: String) = {

        val conn = this.getConn()

        val rs = conn.createStatement().executeQuery(sql)

        val columnCnt: Int = rs.getMetaData.getColumnCount

        val columns: IndexedSeq[String] = 1 to columnCnt map rs.getMetaData.getColumnName
//        val results: Iterator[IndexedSeq[String]] = Iterator.continually(rs).takeWhile(_.next())
//          .map{ rs =>
//            columns map rs.getString
//        }

        val resultSetRow = Iterator.continually((rs.next(), rs)).takeWhile(_._1).map(r => {
            getRowFromResultSet(r._2, columnCnt) // (ResultSet) => (spark.sql.Row)
        })

        val df = Seq(
            (1, "First Value", java.sql.Date.valueOf("2010-01-01")),
            (2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
        )

        println(df)

        resultSetRow.foreach(println(_))
        import spark.implicits._
//        val rdd = resultSetRow.toDF("id", "product", "bt_create_time", "bt_last_time",
//                        "bt_next_time", "effective_end_date", "effective_start_date",
//                        "component", "item", "station")
        println(resultSetRow)
//        resultSetList.toDF("id", "product", "bt_create_time", "bt_last_time",
//                        "bt_next_time", "effective_end_date", "effective_start_date",
//                        "component", "item", "station").show(false)

        //val x = spark.sparkContext.parallelize(resultSetList)

        //        val rows = values.map{x => Row(x:_*)}
//        and then having schema like schema, we can make RDD
//
//        val rdd = sparkContext.makeRDD[RDD](rows)
//        and finally create a spark data frame
//
//        val df = sqlContext.createDataFrame(rdd, schema)

        conn.commit()
//        println(columns)
//        println(results)
//        import spark.implicits._
//        results.toDF("id", "product", "bt_create_time", "bt_last_time",
//            "bt_next_time", "effective_end_date", "effective_start_date",
//            "component", "item", "station").show(false)


        //(columns, results)

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
//                        println("寫入Mariadb筆數 : " + count)
//                        println("sql : " + sql.substring(0, sql.length - 1))
                        conn.createStatement().execute(sql.substring(0, sql.length - 1))
                        count = 0
                        sql = sqlPrefix

                        conn.commit()
                    }
                }

                if(count > 0) {
//                    println("寫入Mariadb筆數 : " + count)
//                    println("sql : " + sql.substring(0, sql.length - 1))
                    conn.createStatement().execute(sql.substring(0, sql.length - 1))
                }

                conn.commit()
            }
        }
    }

    def saveToMariadbUpsertSQL(df: DataFrame, table: String, updateIdx: List[Int], numExecutors: Int): Unit = {
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
        //insert INTO wxj.product_station(product,station_name,flag) VALUES
        //('TaiJi Base','TLEOL','1'), ('TaiJi Base1','TLEOL','1') ON DUPLICATE KEY UPDATE flag='1';
        //stat1 = stat1 + VALUES(stat1), stat2 = stat2 + VALUES(stat2), stat3 = stat3 + VALUES(stat3)
        //INSERT INTO ins_duplicate VALUES (1,'Antelope') ON DUPLICATE KEY UPDATE animal='Antelope';
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

                val conn = DriverManager.getConnection(
                    mariadbUrl,
                    mariadbConnectionProperties)

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
                        //                        println("寫入Mariadb筆數 : " + count)
                        //                        println("sql : " + sql.substring(0, sql.length - 1))
                        conn.createStatement().execute(sql.substring(0, sql.length - 1))
                        count = 0
                        sql = sqlPrefix

                        conn.commit()
                    }
                }

                if(count > 0) {
                    //                    println("寫入Mariadb筆數 : " + count)
                    //                    println("sql : " + sql.substring(0, sql.length - 1))
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
