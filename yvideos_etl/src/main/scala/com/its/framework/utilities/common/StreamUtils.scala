package com.its.framework.utilities.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

object StreamUtils {

  def readKafkaStreams(spark: SparkSession, topic: String, kafkaBootstrapServers:String,options: Map[String, String] = Map()): DataFrame = {

    val reader = spark.readStream

    // Set default options
    reader
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")

    // Apply any additional options from the map
    options.foreach { case (key, value) =>
      reader.option(key, value)
    }
    reader.load()
  }

  def writeStreamParquet(dataframe:DataFrame, dataLoadPath:String,checkPointPath: String,partitionByCols: String, options: Map[String, String] = Map()) = {
   println("Writing from kafka")
    val writer = dataframe.writeStream

    // Set default options
    writer
      .partitionBy(partitionByCols)
      .format("parquet")
      .outputMode("append")
      .option("checkpointLocation", checkPointPath)
      .option("path", dataLoadPath)

    // Apply any additional options from the map
    options.foreach { case (key, value) =>
      writer.option(key, value)
    }
    writer.trigger(Trigger.Once())
      .start()
      .awaitTermination()
  }

}
