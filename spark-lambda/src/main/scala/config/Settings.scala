package config

import com.typesafe.config.ConfigFactory

object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val weblogGen = config.getConfig("flightdelay")

    lazy val kafkaTopic = weblogGen.getString("kafka_topic")
    lazy val hdfsPath = weblogGen.getString("hdfs_path")
  }
}

