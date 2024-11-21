package com.its.framework.config

import com.typesafe.config.{Config, ConfigFactory}

object AppConfig {

  private val env = sys.env.getOrElse("APP_ENV", "prod") // Default to 'dev' if not set
  private val config: Config = ConfigFactory.load(s"$env.conf")

  lazy val GCSLandingBucket: String = config.getString("buckets.landing")
  lazy val GCSOutputBucket: String = config.getString("buckets.output")
  lazy val GCSkafkaLanding: String = config.getString("buckets.kafkalanding")
  lazy val KafkaServers: String = config.getString("kafkaconfs.bootstrapServers")

}
