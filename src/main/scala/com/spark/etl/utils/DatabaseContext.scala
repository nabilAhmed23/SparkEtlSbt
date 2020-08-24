package com.spark.etl.utils

import java.util.Properties

class DatabaseContext(var driver: String,
                      var url: String,
                      var table: String,
                      var username: String,
                      var password: String) {

  val DATABASE_DRIVER: String = driver
  val DATABASE_URL: String = url
  val DATABASE_TABLE: String = table
  val DATABASE_USERNAME: String = username
  val DATABASE_PASSWORD: String = password
  val DATABASE_PROPERTIES = new Properties()
  DATABASE_PROPERTIES.setProperty("user", DATABASE_USERNAME)
  DATABASE_PROPERTIES.setProperty("password", DATABASE_PASSWORD)
}
