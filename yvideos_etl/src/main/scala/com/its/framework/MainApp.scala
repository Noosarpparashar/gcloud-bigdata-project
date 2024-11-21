package com.its.framework

import com.its.framework.jobs.{Job, JobFactory, JobEnum}
import org.apache.spark.sql.SparkSession


object MainApp {


  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Error: Not enough arguments provided.")
      println("Usage: spark-submit --class com.its.framework.MainApp your-application.jar JobTypeName ControllerType")
      System.exit(1)
    }

    val jobType = args(0)
    val controllerType = args(1)
    val jobEnum = JobEnum.withName(jobType)

    try {
      val job: Job = JobFactory.createJob(jobEnum)
      val controller = JobFactory.getController(controllerType)

      // Use the job's specific controller to initialize the Spark session
      implicit val spark: SparkSession = controller.getSparkSession
      try {
        job.runETL()
      } finally {
        spark.stop()
      }
    }
  }
}
