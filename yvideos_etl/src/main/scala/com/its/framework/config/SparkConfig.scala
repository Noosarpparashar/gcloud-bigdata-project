package com.its.framework.config

import org.apache.spark.SparkConf

object SparkConfig {
  def getSparkConf: SparkConf = {
    new SparkConf()
      .set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
      .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
      .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
      .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
  }
}
