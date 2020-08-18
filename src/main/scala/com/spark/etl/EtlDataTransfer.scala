package com.spark.etl

import java.util.Calendar

import com.spark.etl.utils.{DatabaseContext, Utilities}
import org.apache.spark.sql.{SaveMode, SparkSession}

class EtlDataTransfer(session: SparkSession, srcContext: DatabaseContext, destContext: DatabaseContext) extends BaseEtlDataTransfer(session, srcContext, destContext) {

  override def runEtl(): Unit = {
    val pastRead = Calendar.getInstance().getTime
    val tableName = Utilities.getTableAlias(srcContext.DATABASE_TABLE)
    println(s"Before $tableName read: $pastRead")
    val sourceTable = session.read
      .option("driver", srcContext.DATABASE_DRIVER)
      .jdbc(
        url = srcContext.DATABASE_URL,
        table = srcContext.DATABASE_TABLE,
        properties = srcContext.DATABASE_PROPERTIES
      )
    val presentRead = Calendar.getInstance().getTime
    println(s"After $tableName read: $presentRead")

    val pastWrite = Calendar.getInstance().getTime
    println(s"Before ${destContext.DATABASE_TABLE} write: $pastWrite")
    sourceTable.write
      .mode(SaveMode.Append)
      .option("driver", destContext.DATABASE_DRIVER)
      .jdbc(
        url = destContext.DATABASE_URL,
        table = destContext.DATABASE_TABLE,
        connectionProperties = destContext.DATABASE_PROPERTIES
      )
    val presentWrite = Calendar.getInstance().getTime
    println(s"After ${destContext.DATABASE_TABLE} write: $presentWrite")

    val readDifference = Utilities.getTimeDifference(pastRead, presentRead)
    println(s"$tableName read complete in $readDifference")
    val writeDifference = Utilities.getTimeDifference(pastWrite, presentWrite)
    println(s"${destContext.DATABASE_TABLE} write complete in $writeDifference")
  }
}
