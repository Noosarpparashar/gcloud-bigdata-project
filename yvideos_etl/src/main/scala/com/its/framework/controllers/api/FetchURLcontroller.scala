package com.its.framework.controllers.api

import org.apache.spark.sql.SparkSession
import com.its.framework.config.{SparkConfig, SparkSessionProvider}
object FetchURLcontroller  extends  SparkSessionProvider {

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .appName("my-fetch-controller-app")
      .master("local[4]")
      .config(SparkConfig.getSparkConf)
      .config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache.spark=ERROR,console")
      .config("spark.executor.extraJavaOptions", "-Dlog4j.logger.org.apache.spark=ERROR,console")
      .getOrCreate()
  }

}