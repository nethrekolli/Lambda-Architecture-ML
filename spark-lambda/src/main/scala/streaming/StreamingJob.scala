package streaming

import domain.CarriersPerDay
import _root_.kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StateSpec, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import utils.SparkUtils._
import functions._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.sql.SaveMode
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

import scala.util.Try


object StreamingJob {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(4)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)
      val wlc = Settings.WebLogGen
      val topic = wlc.kafkaTopic

      val kafkaDirectParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "lambda",
        "auto.offset.reset" -> "largest"
      )

      var fromOffsets : Map[TopicAndPartition, Long] = Map.empty
      val hdfsPath = wlc.hdfsPath

      Try(sqlContext.read.parquet(hdfsPath)).foreach( hdfsData =>
        fromOffsets = hdfsData.groupBy("topic", "kafkaPartition").agg(max("untilOffset").as("untilOffset"))
          .collect().map { row =>
          (TopicAndPartition(row.getAs[String]("topic"), row.getAs[Int]("kafkaPartition")), row.getAs[String]("untilOffset").toLong + 1)
        }.toMap
      )

      val kafkaDirectStream = fromOffsets.isEmpty match {
        case true =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaDirectParams, Set(topic)
          )
        case false =>
          KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
            ssc, kafkaDirectParams, fromOffsets, { mmd: MessageAndMetadata[String, String] => (mmd.key(), mmd.message()) }
          )
      }

      val FlightDelayStream = kafkaDirectStream.transform(input => {
        functions.rddToRDDFlightDelay(input)
      }).cache()

      // save data to HDFS
      FlightDelayStream.foreachRDD { rdd =>
        val flightDelayDF = rdd
          .toDF()
          .selectExpr("arr_delay", "carrier", "day_of_month", "day_of_week", "day_of_year", "dep_delay", "dest", "distance", "flight_date", "flight_num", "origin", "route", "arr_delay_est", "arr_time", "dep_time", "inputProps.topic as topic", "inputProps.kafkaPartition as kafkaPartition", "inputProps.fromOffset as fromOffset", "inputProps.untilOffset as untilOffset")

        flightDelayDF
          .write
          .partitionBy("topic", "kafkaPartition", "carrier")
          .mode(SaveMode.Append)
          .parquet(hdfsPath)
      }


      // unique visitors by product
      val carrierStateSpec =
        StateSpec
          .function(mapCarriersStateFunc)
          .timeout(Minutes(120))

      val statefulCarriersPerDay = FlightDelayStream.map(a => {
        val hll = new HyperLogLogMonoid(12)
        ((a.route, a.flight_date), hll(a.carrier.getBytes))
      }).mapWithState(carrierStateSpec)

      val carrierStateSnapshot = statefulCarriersPerDay.stateSnapshots()
      carrierStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)
        ) // only save or expose the snapshot every x seconds
        .map(sr => CarriersPerDay(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))
        .saveToCassandra("lambda", "stream_carriers_per_day")

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()

  }

}
