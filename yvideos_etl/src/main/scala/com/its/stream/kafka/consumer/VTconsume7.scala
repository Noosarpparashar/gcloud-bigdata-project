package com.its.stream.kafka.consumer

import com.its.framework.config.SparkSessionProvider
import org.apache.spark.sql.functions.{col, json_tuple, regexp_extract, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.its.framework.jobs.Job
import com.its.framework.config.AppConfig._
import com.its.framework.utilities.common.StreamUtils


class VTconsume7(controller: SparkSessionProvider) extends Job{



  implicit val spark: SparkSession = controller.getSparkSession
  spark.conf.getAll.foreach(println)
  // Execute the ETL process

  val gcsCheckPointPath = s"$GCSkafkaLanding/vt/checkpoints1"
  val gcsLoadPath = s"$GCSkafkaLanding/vt/delta1"
  val kafkaBootstrapServers = s"$KafkaServers"
  val topic = "vt"

  override def extract(): Map[String, DataFrame] = {


    val kafkaOptions: Map[String, String] = Map(
      "startingOffsets" -> "earliest",          // Set startingoffsets as lates or earlies
    )
    val kafkaDF = StreamUtils.readKafkaStreams(spark,topic,kafkaBootstrapServers,kafkaOptions)

    Map("kafkaDF" -> kafkaDF)

  }

  override def transform(dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    val kafkaDF = dfs("kafkaDF")

    val jsonParsedDF = kafkaDF
      .selectExpr("CAST(value as STRING) as jsonString") // Cast Kafka value to a string
      .withColumn("jsonString", regexp_extract(col("jsonString"), "\\{.*\\}", 0))
      .select(
        json_tuple(col("jsonString"), "video_id", "title", "load_date")
      ).toDF("video_id", "title", "load_date")
    StreamUtils.writeStreamParquet(jsonParsedDF,gcsLoadPath,gcsCheckPointPath,partitionByCols = "load_date")


    Map("jsonParsedDF" -> jsonParsedDF)

  }

  override def load(dfs: Map[String, DataFrame]): Unit = {

  }

  override def runETL(): Unit = {
    val data = extract()
    transform(data)
   // load(transformedData)
  }

}