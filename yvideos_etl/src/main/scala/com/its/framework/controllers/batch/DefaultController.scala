package com.its.framework.controllers.batch

import org.apache.spark.sql.SparkSession
import com.its.framework.config.SparkConfig
object DefaultController {

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .config(SparkConfig.getSparkConf)
      .getOrCreate()
  }

}
