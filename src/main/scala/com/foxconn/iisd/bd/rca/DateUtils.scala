package com.foxconn.iisd.bd.rca

import java.text.SimpleDateFormat
import java.util.Date

import com.foxconn.iisd.bd.rca.XWJBigtable.configLoader

object DateUtils {
    val DATE_FORMAT = configLoader.getString("log_prop", "product_dt_fmt")

    def getDateAsString(d: Date): String = {
        val dateFormat = new SimpleDateFormat(DATE_FORMAT)
        dateFormat.format(d)
    }

    def convertStringToDate(s: String): Date = {
        val dateFormat = new SimpleDateFormat(DATE_FORMAT)
        dateFormat.parse(s)
    }

}
