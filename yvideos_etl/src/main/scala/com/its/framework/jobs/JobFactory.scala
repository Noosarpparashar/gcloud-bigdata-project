package com.its.framework.jobs

import com.its.framework.config.SparkSessionProvider
import com.its.batch.VideoProcessingJob
import com.its.api.FetchURLJob
import com.its.framework.controllers.batch.VideoProcessingController
import com.its.framework.controllers.api.FetchURLcontroller
import com.its.framework.controllers.stream.{VTconsume7controller, VVLDconsumecontroller}
import com.its.stream.kafka.consumer.{VTconsume7, VVLDconsume}


object JobFactory {

  def createJob(jobType: JobEnum.JobEnum): Job  = {
    jobType match {
      case JobEnum.VideoProcessingJob => new VideoProcessingJob(VideoProcessingController)
      case JobEnum.FetchURLJob => new FetchURLJob(FetchURLcontroller)
      case JobEnum.VTconsume7 => new VTconsume7(VTconsume7controller)
      case JobEnum.VVLDconsume => new VVLDconsume(VVLDconsumecontroller)

      case _ =>
        throw new IllegalArgumentException("Invalid job type")
    }
  }

  def getController(controllerType: String): SparkSessionProvider = {
    controllerType match {
      case "VideoProcessingController" => VideoProcessingController
      case "FetchURLcontroller" => FetchURLcontroller
      case "VTconsume7controller" => VTconsume7controller
      case "VVLDconsumecontroller" => VVLDconsumecontroller
      case _ => throw new IllegalArgumentException("Invalid controller type")
    }
  }


}
