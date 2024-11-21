package com.its.framework.jobs

object JobEnum extends Enumeration {
  type JobEnum = Value

/*
  List of Batch Jobs used in the project
  */
    val VideoProcessingJob: Value = Value("VideoProcessingJob")
    val FetchURLJob: Value =Value("FetchURLJob")
  //  val Job5: Value = Value("Job5")
  //  val Job6: Value = Value("Job6")
  /*
  List of Stream Jobs used in the project
  */
  val VTconsume7: Value =Value("VTconsume7")
  val VVLDconsume: Value =Value("VVLDconsume")

  def getJobName(job: JobEnum): String = job.toString
}
