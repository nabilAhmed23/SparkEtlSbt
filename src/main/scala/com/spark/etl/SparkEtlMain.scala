package com.spark.etl

import java.io.{FileNotFoundException, FileReader}
import java.util.Properties

import com.spark.etl.utils.{DatabaseContext, Utilities}
import org.apache.spark.sql.SparkSession

object SparkEtlMain {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder()
      .config("spark.scheduler.mode", "FAIR")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    try {
      if (args.length != 3) {
        throw new Exception(s"Incorrect number of arguments. Expected 3 (properties file, source table, destination table), got ${args.length}")
      }
      val reader = new FileReader(args(0))
      val properties = new Properties()
      properties.load(reader)

      if (!Utilities.validateDatabaseProperties(properties)) {
        throw new Exception(s"File ${args(0)} missing one of:" +
          s"\n${Utilities.SRC_DRIVER_PROPERTY}" +
          s"\n${Utilities.SRC_URL_PROPERTY}" +
          s"\n${Utilities.SRC_USERNAME_PROPERTY}" +
          s"\n${Utilities.SRC_PASSWORD_PROPERTY}" +
          s"\n${Utilities.DEST_DRIVER_PROPERTY}" +
          s"\n${Utilities.DEST_URL_PROPERTY}" +
          s"\n${Utilities.DEST_USERNAME_PROPERTY}" +
          s"\n${Utilities.DEST_PASSWORD_PROPERTY}")
      }

      println(s"File ${args(0)} properties:")
      properties.stringPropertyNames().forEach(prop => println(s"$prop: ${properties.getProperty(prop)}"))

      val SRC_DRIVER = properties.getProperty(Utilities.SRC_DRIVER_PROPERTY)
      val SRC_URL = properties.getProperty(Utilities.SRC_URL_PROPERTY)
      val SRC_USERNAME = properties.getProperty(Utilities.SRC_USERNAME_PROPERTY)
      val SRC_PASSWORD = properties.getProperty(Utilities.SRC_PASSWORD_PROPERTY)
      val SRC_TABLE = Utilities.addTableAlias(args(1).trim)
      val DEST_DRIVER = properties.getProperty(Utilities.DEST_DRIVER_PROPERTY)
      val DEST_URL = properties.getProperty(Utilities.DEST_URL_PROPERTY)
      val DEST_USERNAME = properties.getProperty(Utilities.DEST_USERNAME_PROPERTY)
      val DEST_PASSWORD = properties.getProperty(Utilities.DEST_PASSWORD_PROPERTY)
      val DEST_TABLE = args(2).trim

      println(s"Source table: ${args(1).trim}")
      println(s"Destination table: ${args(2).trim}")

      new EtlDataTransfer(session,
        new DatabaseContext(SRC_DRIVER, SRC_URL, SRC_TABLE, SRC_USERNAME, SRC_PASSWORD),
        new DatabaseContext(DEST_DRIVER, DEST_URL, DEST_TABLE, DEST_USERNAME, DEST_PASSWORD))
        .runEtl()
    } catch {
      case e: FileNotFoundException => println(s"File not found ${args(0)}")
      case e: Exception => println(s"Exception thrown: $e")
    } finally {
      session.stop()
    }
  }
}
