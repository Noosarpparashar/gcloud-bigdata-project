package com.its.api

import com.its.framework.config.SparkSessionProvider
import com.its.framework.controllers.api.FetchURLcontroller
import com.its.framework.utilities.common.DataFrameUtils
import com.its.framework.utilities.common.DataFrameUtils.writeParquet
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import sttp.client3._
import play.api.libs.json._
import com.its.framework.jobs.Job
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class FetchURLJob (controller: SparkSessionProvider) extends Job{

  println("*************************************")


  implicit val spark: SparkSession = controller.getSparkSession
  spark.conf.getAll.foreach(println)

  case class VideoIdsRequest(videoIds: Seq[String])
  case class VideoUrl(videoId: String, url: String)

  implicit val videoIdsRequestFormat: Format[VideoIdsRequest] = Json.format[VideoIdsRequest]
  implicit val videoUrlFormat: Format[VideoUrl] = Json.format[VideoUrl]

  val backend = HttpURLConnectionBackend()

  def fetchUrlsForBatch(videoIds: Seq[String]): Seq[(String, String)] = {
    val requestBody = Json.toJson(VideoIdsRequest(videoIds)).toString()
    val requestUrl = uri"http://localhost:8080/api/v1/videos/urls"
    val response = basicRequest
      .body(requestBody)
      .contentType("application/json")
      .post(requestUrl)
      .send(backend)


    response.body match {
      case Right(responseBody) =>
        Json.parse(responseBody).validate[Seq[VideoUrl]] match {
          case JsSuccess(videoUrls, _) =>
            videoUrls.collect { case VideoUrl(videoId, url) => (videoId, url) }
          case JsError(errors) =>
            println(s"Failed to parse JSON: $errors")
            Seq.empty
        }
      case Left(error) =>
        println(s"Request failed: $error")
        Seq.empty
    }
  }

  override def extract(): Map[String, DataFrame] = {


    val GCSLandingPath = "gs://yvideos_gcp_poc/landing/USvideos.csv"
    val csvOptions: Map[String, String] = Map(
      "delimiter" -> ",", // Define the delimiter if needed
      "inferSchema" -> "true", // Override to disable schema inference
    )
    val videoIds = DataFrameUtils.readCSV(spark, GCSLandingPath, csvOptions).select("video_id")
    Map("videoIds" -> videoIds)
  }

  val urlSchema = StructType(List(
    StructField("videoId", StringType, nullable = true),
    StructField("url", StringType, nullable = true)
  ))

  def intermediateload(dfs: Map[String, DataFrame]): Unit = {

    val GCSOutputPath = "gs://yvideos_gcp_poc/targetApi"
    val processedLandingDF = dfs("videoUrlsDf")

    processedLandingDF.show()
    val parquetOptions: Map[String, String] = Map(
      "mode" -> "append", // Define the delimiter if needed
      "compression" -> "snappy",
    )
    processedLandingDF.printSchema()
    writeParquet(processedLandingDF, GCSOutputPath, parquetOptions)


  }

  def transform1(spark: SparkSession, dfs: Map[String, DataFrame]) = {
    import spark.implicits._
    val videoIds = dfs("videoIds").as[String].collect()

    videoIds.grouped(100).foreach { batch =>
      val videoUrls: Seq[(String, String)] = fetchUrlsForBatch(batch) // Fetch URLs for each batch
      val rows = videoUrls.map { case (videoId, url) => Row(videoId, url) }
      val videoUrlsDf = spark.createDataFrame(spark.sparkContext.parallelize(rows), urlSchema)
      videoUrlsDf.show()

      intermediateload(Map("videoUrlsDf" -> videoUrlsDf))

    }


  }
  override def transform(dfs: Map[String, org.apache.spark.sql.DataFrame]): Map[String, org.apache.spark.sql.DataFrame] = {
    Map ("df" ->spark.emptyDataFrame)
  }

  override def load(dfs: Map[String, org.apache.spark.sql.DataFrame]) = {

  }


  override def runETL(): Unit = {
    val data = extract()
    transform1(spark,data)
  }
}
