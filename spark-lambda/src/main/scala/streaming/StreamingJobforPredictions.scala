package streaming

import com.mongodb.spark.MongoSpark
import kafka.serializer.StringDecoder
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import utils.SparkUtils.{getSQLContext, getSparkContext, getStreamingContext}
import domain._

import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.PipelineModel

object StreamingJobforPredictions {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(10)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)
      val topic = "flight_delay_classification_request"
      val kafkaDirectParamsforWebRequest = Map(
        "metadata.broker.list" -> "localhost:9092",
        "group.id" -> "web",
        "auto.offset.reset" -> "largest"
      )

      val pipelineModel = PipelineModel.load("C:\\Users\\Veda\\Desktop\\models\\random_classifier_model")

      val KafkaDirectStreamforPrediction = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaDirectParamsforWebRequest, Set(topic)
      ).map(_._2)

      KafkaDirectStreamforPrediction.print()

      var value = new ListBuffer[String]()
      val predictionStream = KafkaDirectStreamforPrediction.transform(input=>{
        input.flatMap{ line =>
          val record = line.split(", ")
          for(i <- 0 to 13){
            val words = record(i).split(": ")
            if(words(1) contains "\""){
              val result = words(1).slice(1, words(1).length-1)
              value += result.toString
            }
            else{
              value += words(1).toString
            }
          }
          val values = value.toList
          print(values)
          if(values.length==14)
            Some(Predictions(values(0).toInt, values(1), values(2), values(3), values(4), values(5), values(6), values(7).toInt, values(8).toInt, values(9).toDouble, values(10).toDouble, values(11).toDouble, values(12), values(13)))
          else
            None
        }
      })

      predictionStream.foreachRDD(rdd =>{
        val predictionDF = rdd.toDF()
        predictionDF.show()
        val results = pipelineModel.transform(predictionDF)
        results.show()
        val final_predictions = results.drop("features").drop("rawPrediction").drop("probability")
        final_predictions.show()
        if(final_predictions.count() > 0){
          final_predictions
            .write
            .mode("append")
            .format("org.apache.spark.sql.cassandra")
            .options(Map( "keyspace" -> "lambda", "table" -> "flight_delay_classification_response"))
            .save()
        }
      })

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    ssc.start()
    ssc.awaitTermination()
  }
}
