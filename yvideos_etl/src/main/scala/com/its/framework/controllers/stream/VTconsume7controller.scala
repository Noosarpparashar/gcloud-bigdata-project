package com.its.framework.controllers.stream

import org.apache.spark.sql.SparkSession
import com.its.framework.config.{SparkConfig, SparkSessionProvider}
object VTconsume7controller extends  SparkSessionProvider{

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("vt-consume7-controller")
      .master("local[*]")
      .config(SparkConfig.getSparkConf)
      .getOrCreate()
  }

}

