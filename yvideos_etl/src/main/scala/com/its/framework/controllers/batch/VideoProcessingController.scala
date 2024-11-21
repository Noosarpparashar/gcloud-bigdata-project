package com.its.framework.controllers.batch

import org.apache.spark.sql.SparkSession
import com.its.framework.config.{SparkConfig, SparkSessionProvider}
object VideoProcessingController extends  SparkSessionProvider{

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("my-fetch-controller-app")
      .master("local[*]")
      .config(SparkConfig.getSparkConf)
      .getOrCreate()
  }

}
