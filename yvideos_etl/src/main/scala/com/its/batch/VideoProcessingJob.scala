package com.its.batch

import com.its.framework.config.SparkSessionProvider
import com.its.framework.utilities.common.DataFrameUtils
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.its.framework.controllers.batch.VideoProcessingController
import com.its.framework.jobs.Job
import com.its.framework.utilities.common.DataFrameUtils._
import com.its.framework.config.AppConfig._
import com.its.framework.utilities.connectors.iceberg.IcebergConnector._



class VideoProcessingJob(controller: SparkSessionProvider) extends Job{



  implicit val spark: SparkSession = controller.getSparkSession
  spark.conf.getAll.foreach(println)
  // Execute the ETL process

  override def extract(): Map[String, DataFrame] = {

    val GCSLandingPath = s"$GCSLandingBucket/USvideos.csv"
    println("****", GCSLandingPath)

    val csvOptions: Map[String, String] = Map(
      "delimiter" -> ",",          // Define the delimiter if needed
      "inferSchema" -> "true",    // Override to disable schema inference

    )
    val landingDF = DataFrameUtils.readCSV(spark, GCSLandingPath, csvOptions)

    Map("landing" -> landingDF)

  }

  override def transform(dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    val landingDF = dfs("landing")

    val processedLandingDF = landingDF
      .withColumn("load_date", to_date(col("trending_date"), "yy.dd.MM"))
      .select("video_id", "views", "likes", "dislikes", "load_date")
      .filter(col("load_date") === "2017-11-15")

    Map("processedLandingDF" -> processedLandingDF)

  }

  override def load(dfs: Map[String, DataFrame]): Unit = {

    import java.time.LocalDateTime
    import java.time.format.DateTimeFormatter

    // Define a timestamp format
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
    val timestamp = LocalDateTime.now().format(formatter)

    val GCSOutputPath = "gs://yvideos_gcp_poc/target6"

  //  val GCSOutputPath = s"src/main/scala/com/its/output/$timestamp"

    val processedLandingDF = dfs("processedLandingDF")

    processedLandingDF.show()
    val parquetOptions: Map[String, String] = Map(
      "mode" -> "append",          // Define the delimiter if needed
      "compression" -> "snappy",
    )
    processedLandingDF.printSchema()
    writeParquet(processedLandingDF, GCSOutputPath, parquetOptions)
    val tablename = "spark_catalog.default.yvideos_vvld_delta"
    writeTable(spark,processedLandingDF,tablename)


  }

  override def runETL(): Unit = {
    val data = extract()
    val transformedData = transform(data)
    load(transformedData)
  }

}
