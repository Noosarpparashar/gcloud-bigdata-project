package com.its.framework.controllers.stream

import org.apache.spark.sql.SparkSession
import com.its.framework.config.{SparkConfig, SparkSessionProvider}
object VVLDconsumecontroller extends  SparkSessionProvider{

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("vvld-consume-controller")
      .master("local[*]")
      .config(SparkConfig.getSparkConf)
      .getOrCreate()
  }

}
