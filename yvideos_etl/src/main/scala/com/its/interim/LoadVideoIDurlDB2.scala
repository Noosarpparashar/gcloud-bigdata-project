package com.its.interim

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object LoadVideoIDurlDB2 extends  App{

  val conf = new SparkConf()
    .setAppName("ReadParquet")
    .set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    .set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    .set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    .set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    .set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true")


  val spark = SparkSession.builder
    .master("local[*]").config(conf).getOrCreate()

  // Specify the GCS bucket path where the data should be written
  //val gcslandingPath = "gs://yvideos_gcp_poc/landing/USvideos.csv"
  val GCSLandingPath = "gs://yvideos_gcp_poc/landing/USvideos.csv"


  // Your DataFrame (assuming df is already created)
  val videoUrlsDF = spark.read.option("header", "true")
    .option("inferSchema", "true")
    .option("quote", "\"")
    .option("escape", "\\")
    .option("multiline", "true")
    .csv(GCSLandingPath)
    .select("video_id", "thumbnail_link")
    .withColumnRenamed("thumbnail_link", "url")

  // PostgreSQL connection properties
  val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres" // Replace with your actual DB URL
  val connectionProperties = new java.util.Properties()
  connectionProperties.setProperty("user", "prasoon") // Replace with your DB username
  connectionProperties.setProperty("password", "9473") // Replace with your DB password
  connectionProperties.setProperty("driver", "org.postgresql.Driver")

  // Write DataFrame to PostgreSQL (replace "public.video_urls" with your table name)
  videoUrlsDF.dropDuplicates.write
    .mode("append") // Choose write mode: overwrite, append, etc.
    .jdbc(jdbcUrl, "public.YVIDEO_API_URL", connectionProperties)

  // Stop Spark session
  spark.stop()

}

