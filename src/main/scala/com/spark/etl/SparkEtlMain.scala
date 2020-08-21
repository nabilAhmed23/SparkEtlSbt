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
      val properties = new Properties()
      properties.load(new FileReader(args(0).trim))

      if (!Utilities.validateDatabaseProperties(properties)) {
        throw new Exception(s"File ${args(0).trim} missing one of:" +
          s"\n${Utilities.SRC_DRIVER_PROPERTY}" +
          s"\n${Utilities.SRC_URL_PROPERTY}" +
          s"\n${Utilities.SRC_USERNAME_PROPERTY}" +
          s"\n${Utilities.SRC_PASSWORD_PROPERTY}" +
          s"\n${Utilities.DEST_DRIVER_PROPERTY}" +
          s"\n${Utilities.DEST_URL_PROPERTY}" +
          s"\n${Utilities.DEST_USERNAME_PROPERTY}" +
          s"\n${Utilities.DEST_PASSWORD_PROPERTY}")
      }

      println("=================================================================\n")
      println(s"File ${args(0).trim} properties:")
      properties.stringPropertyNames().forEach(prop => println(s"$prop: ${properties.getProperty(prop)}"))
      println("=================================================================\n")

      println("=================================================================\n")
      println(s"Source table: ${args(1).trim}")
      println(s"Destination table: ${args(2).trim}")
      println("=================================================================\n")

      new EtlDataTransfer(session,
        new DatabaseContext(properties.getProperty(Utilities.SRC_DRIVER_PROPERTY).trim,
          properties.getProperty(Utilities.SRC_URL_PROPERTY).trim,
          Utilities.addTableAlias(args(1).trim).trim,
          properties.getProperty(Utilities.SRC_USERNAME_PROPERTY).trim,
          properties.getProperty(Utilities.SRC_PASSWORD_PROPERTY).trim),
        new DatabaseContext(properties.getProperty(Utilities.DEST_DRIVER_PROPERTY).trim,
          properties.getProperty(Utilities.DEST_URL_PROPERTY).trim,
          args(2).trim,
          properties.getProperty(Utilities.DEST_USERNAME_PROPERTY).trim,
          properties.getProperty(Utilities.DEST_PASSWORD_PROPERTY).trim))
        .runEtl()
    } catch {
      case e: FileNotFoundException => println(s"File not found ${args(0).trim}: $e")
      case e: Exception => println(s"Exception thrown: $e")
    } finally {
      session.stop()
      println("=================================================================\n")
      println("Application terminated")
    }
  }
}
