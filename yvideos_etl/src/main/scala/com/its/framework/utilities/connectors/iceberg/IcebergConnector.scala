package com.its.framework.utilities.connectors.iceberg

import org.apache.spark.sql.{DataFrame, SparkSession}

object IcebergConnector {

  // Method to read data from an Iceberg table
  def readTable(spark: SparkSession, tableName: String): DataFrame = {
    // Assuming the Iceberg table is available in the Spark session
    spark.read.format("iceberg").load(tableName)
  }

  // Method to write data to an Iceberg table
  def writeTable(spark: SparkSession, df: DataFrame, tableName: String, mode: String = "append"): Unit = {
    df.write
      .format("iceberg")
      .mode(mode) // append, overwrite, etc.
      .save(tableName)
  }

  // Method to create a new Iceberg table with specified schema (if not exists)
  def createTableIfNotExists(spark: SparkSession, tableName: String, schema: String): Unit = {
    val sqlCreateTable = s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        $schema
      )
      USING iceberg
    """
    spark.sql(sqlCreateTable)
  }

  // Method to update a table's metadata (e.g., add partitioning or change schema)
  def alterTable(spark: SparkSession, tableName: String, sqlAlter: String): Unit = {
    val sql = s"ALTER TABLE $tableName $sqlAlter"
    spark.sql(sql)
  }
}
