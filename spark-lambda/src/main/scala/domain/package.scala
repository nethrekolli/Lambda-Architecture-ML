
package object domain {
  case class FlightDelay(arr_delay: Int,
                         carrier: String,
                         day_of_month: Int,
                         day_of_week: Int,
                         day_of_year: Int,
                         dep_delay: Int,
                         dest: String,
                         distance: Int,
                         flight_date: String,
                         origin: String,
                         route: String,
                         arr_delay_est: Int,
                         arr_time: Int,
                         dep_time: Int,
                         flight_num: String,
                         inputProps: Map[String, String] = Map()
                        )

  case class Predictions(dep_delay: Int,
                         carrier: String,
                         flight_date: String,
                         dest: String,
                         flight_num: String,
                         origin: String,
                         route: String,
                         arr_time: Int,
                         dep_time: Int,
                         distance: Double,
                         day_of_month: Double,
                         day_of_week: Double,
                         uuid: String,
                         timestamp: String
                        )

  case class CarriersPerDay (route: String, flight_date : String, no_of_carriers : Long)
}
