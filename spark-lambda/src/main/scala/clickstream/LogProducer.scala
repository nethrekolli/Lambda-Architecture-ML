package clickstream

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import config.Settings

import scala.io.Source

object LogProducer extends App {
  // WebLog config
  val wlc = Settings.WebLogGen

  val topic = wlc.kafkaTopic
  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "FlightDelay")

  val kafkaProducer: Producer[Nothing, String] = new KafkaProducer[Nothing, String](props)
  println(kafkaProducer.partitionsFor(topic))

  val filename = "C:\\Users\\Veda\\Downloads\\data.csv"
  for (line <- Source.fromFile(filename).getLines()) {
    val words = line.split(",")
    val output = words(0)+"\t"+words(1)+"\t"+words(2)+"\t"+words(3)+"\t"+words(4)+"\t"+words(5)+"\t"+words(6)+"\t"+words(7)+"\t"+words(8)+"\t"+words(9)+"\t"+words(10)+"\t"+words(11)+"\t"+words(12)+"\t"+words(13)+"\t"+words(14)+"\n"
    val producerRecord = new ProducerRecord(topic, output)
    kafkaProducer.send(producerRecord)
    val sleeping = 1000
    print(s"Sleeping $sleeping ms\n")
    Thread sleep sleeping
  }

  kafkaProducer.close()
}


