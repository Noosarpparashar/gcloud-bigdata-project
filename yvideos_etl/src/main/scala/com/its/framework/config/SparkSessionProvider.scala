package com.its.framework.config

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  def getSparkSession: SparkSession
}
