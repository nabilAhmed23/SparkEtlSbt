package com.spark.etl

import com.spark.etl.utils.DatabaseContext
import org.apache.spark.sql.SparkSession

abstract class BaseEtlDataTransfer(val session: SparkSession,
                                   val srcContext: DatabaseContext,
                                   val destContext: DatabaseContext) {

  def runEtl(): Unit
}
