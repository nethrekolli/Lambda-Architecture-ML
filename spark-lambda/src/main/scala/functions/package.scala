import com.twitter.algebird.{HLL, HyperLogLogMonoid}
import domain.FlightDelay
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.State
import org.apache.spark.streaming.kafka.HasOffsetRanges

package object functions {
  def rddToRDDFlightDelay(input: RDD[(String, String)]) = {
    val offsetRanges = input.asInstanceOf[HasOffsetRanges].offsetRanges

    input.mapPartitionsWithIndex( {(index, it) =>
      val or = offsetRanges(index)
      it.flatMap { kv =>
        val line = kv._2
        val record = line.split("\\t")
        if (record.length == 15)
          Some(FlightDelay(record(0).toInt, record(1), record(2).toInt, record(3).toInt, record(4).toInt, record(5).toInt, record(6), record(7).toInt, record(8), record(9), record(10), record(11).toInt, record(12).toInt, record(13).toInt, record(14),
            Map("topic"-> or.topic, "kafkaPartition" -> or.partition.toString,
              "fromOffset" -> or.fromOffset.toString, "untilOffset" -> or.untilOffset.toString)))
        else
          None
      }
    })
  }


  def mapCarriersStateFunc = (k: (String, String), v: Option[HLL], state: State[HLL]) => {
    val currentCarrierHLL = state.getOption().getOrElse(new HyperLogLogMonoid(12).zero)
    val newCarrierHLL = v match {
      case Some(carrierHLL) => currentCarrierHLL + carrierHLL
      case None => currentCarrierHLL
    }
    state.update(newCarrierHLL)
    val output = newCarrierHLL.approximateSize.estimate
    output
  }

}
