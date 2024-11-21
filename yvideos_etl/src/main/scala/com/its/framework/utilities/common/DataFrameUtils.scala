package com.its.framework.utilities.common

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameUtils {

  def readCSV(spark: SparkSession, path: String, options: Map[String, String] = Map()): DataFrame = {
    println("***", path)
    val reader = spark.read

    // Set default options
    reader.option("header", "true")
      .option("inferSchema", "true")
      .option("quote", "\"")
      .option("escape", "\\")
      .option("multiline", "true")

    // Apply any additional options from the map
    options.foreach { case (key, value) =>
      reader.option(key, value)
    }

    reader.csv(path)
  }

  def writeParquet(df: DataFrame, path: String, options: Map[String, String] = Map()): Unit = {
    val writer = df.write

    val mode = options.get("mode").getOrElse("append")
    writer.mode(mode) // Use the provided mode or default to "append"

    // Apply any additional options from the map
    options.filterNot(_._1 == "mode").foreach { case (key, value) =>
      writer.option(key, value)
    }
    options.foreach { case (key, value) =>
      writer.option(key, value)
    }

    // Write the DataFrame to Parquet format
    writer.parquet(path)
  }



}
