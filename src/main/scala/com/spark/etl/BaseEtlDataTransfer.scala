package com.spark.etl

import com.spark.etl.utils.DatabaseContext
import org.apache.spark.sql.SparkSession

abstract class BaseEtlDataTransfer(session: SparkSession, srcContext: DatabaseContext, destContext: DatabaseContext) {
  def runEtl(): Unit
}
