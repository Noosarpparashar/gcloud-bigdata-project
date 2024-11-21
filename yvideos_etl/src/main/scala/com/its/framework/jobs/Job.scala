package com.its.framework.jobs

import com.its.framework.controllers.batch.DefaultController
import org.apache.spark.sql.SparkSession

trait Job {
  implicit val spark: SparkSession
  def extract(): Map[String, org.apache.spark.sql.DataFrame]
  def transform(dfs: Map[String, org.apache.spark.sql.DataFrame]): Map[String, org.apache.spark.sql.DataFrame]
  def load(dfs: Map[String, org.apache.spark.sql.DataFrame]): Unit

  // Default runETL method
  def runETL(): Unit = {
    val extractedData = extract()
    val transformedData = transform(extractedData)
    load(transformedData)
  }

}
