package com.spark.etl.utils

import java.util.concurrent.TimeUnit
import java.util.{Date, Properties}

object Utilities {

  val SRC_DRIVER_PROPERTY = "source.database.driver"
  val SRC_URL_PROPERTY = "source.database.url"
  val SRC_USERNAME_PROPERTY = "source.database.username"
  val SRC_PASSWORD_PROPERTY = "source.database.password"
  val DEST_DRIVER_PROPERTY = "destination.database.driver"
  val DEST_URL_PROPERTY = "destination.database.url"
  val DEST_USERNAME_PROPERTY = "destination.database.username"
  val DEST_PASSWORD_PROPERTY = "destination.database.password"

  def validateDatabaseProperties(properties: Properties): Boolean = {
    val propertyKeys = properties.stringPropertyNames()

    propertyKeys.contains(SRC_DRIVER_PROPERTY) &&
      propertyKeys.contains(SRC_URL_PROPERTY) &&
      propertyKeys.contains(SRC_USERNAME_PROPERTY) &&
      propertyKeys.contains(SRC_PASSWORD_PROPERTY) &&
      propertyKeys.contains(DEST_DRIVER_PROPERTY) &&
      propertyKeys.contains(DEST_URL_PROPERTY) &&
      propertyKeys.contains(DEST_USERNAME_PROPERTY) &&
      propertyKeys.contains(DEST_PASSWORD_PROPERTY)
  }

  def getTimeDifference(past: Date, present: Date): String = {
    val difference = present.getTime - past.getTime
    var timeDifference = ""
    if (TimeUnit.MILLISECONDS.toHours(difference) > 0) {
      timeDifference += s"${TimeUnit.MILLISECONDS.toHours(difference)} hours, "
    }
    if (TimeUnit.MILLISECONDS.toMinutes(difference) > 0) {
      timeDifference += s"${TimeUnit.MILLISECONDS.toMinutes(difference)} minutes, "
    }
    timeDifference += s"${TimeUnit.MILLISECONDS.toSeconds(difference)} seconds"

    timeDifference
  }

  def addTableAlias(srcTable: String): String = {
    var tableName = srcTable.trim
    if (tableName.indexOf(" ") > -1 && tableName.substring(0, tableName.indexOf(" ")).toUpperCase == "SELECT") {
      tableName = s"($tableName) source_table"
    }

    tableName
  }

  def getTableAlias(srcTable: String): String = {
    var tableName = srcTable.trim
    if (tableName.indexOf(" ") > -1 && tableName.substring(1, tableName.indexOf(" ")).toUpperCase == "SELECT") {
      tableName = tableName.substring(tableName.lastIndexOf(" ") + 1, tableName.length).trim
    }

    tableName
  }
}
