package batch

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import com.datastax.spark.connector.SomeColumns
import config.Settings
import org.apache.spark.sql.{DataFrame, SaveMode}
import domain._
import utils.SparkUtils._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.MulticlassMetrics

object BatchJob {

  def main(args: Array[String]): Unit = {

    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    val wlc = Settings.WebLogGen

    // initialize input RDD
    val inputDF = sqlContext.read.parquet("hdfs://localhost:9000/lambda/flightdelay-app1/")

    inputDF.createOrReplaceTempView("flightdelay")
    val carriersPerDay = sqlContext.sql(
      """SELECT route, flight_date, COUNT(DISTINCT carrier) as no_of_carriers
        |FROM flightdelay GROUP BY route, flight_date
      """.stripMargin)

    carriersPerDay
      .write
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "keyspace" -> "lambda", "table" -> "batch_carriers_per_day"))
      .save()

    val classifierdata = inputDF.selectExpr("cast(arr_delay_est as Double) label", "cast(day_of_month as Double) day_of_month", "cast(day_of_week as Double) day_of_week", "dep_delay", "cast(distance as Double) distance", "carrier", "origin", "dest", "route", "dep_time", "arr_time")
    classifierdata.printSchema()
    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("label_index").fit(classifierdata)
    val label = labelIndexer.transform(classifierdata)
    label.show()
    val carrierIndexer = new StringIndexer().setInputCol("carrier").setOutputCol("carrier_index").fit(classifierdata)
    val originIndexer = new StringIndexer().setInputCol("origin").setOutputCol("origin_index").fit(classifierdata)
    val destIndexer = new StringIndexer().setInputCol("dest").setOutputCol("dest_index").fit(classifierdata)
    val routeIndexer = new StringIndexer().setInputCol("route").setOutputCol("route_index").fit(classifierdata)


    val assembler = new VectorAssembler().setInputCols(Array("day_of_month", "day_of_week", "dep_delay", "distance", "carrier_index", "origin_index", "dest_index", "route_index", "dep_time", "arr_time")).setOutputCol("features")

    val Array(training, test) = classifierdata.randomSplit(Array(0.7,0.3))

    val rfclassifier = new RandomForestClassifier().setLabelCol("label_index").setFeaturesCol("features").setNumTrees(10).setMaxBins(4657)

    val pipeline = new Pipeline().setStages(Array(labelIndexer, carrierIndexer, originIndexer, destIndexer, routeIndexer, assembler, rfclassifier))

    val model = pipeline.fit(training)

    val results = model.transform(test)

    val PredictionAndLabels = results.select("prediction", "label").rdd.map(r=>(r.getDouble(0), r.getDouble(1)))

    val metrics = new MulticlassMetrics(PredictionAndLabels)

    println(metrics.confusionMatrix)

    model.write.overwrite().save("C:\\Users\\Veda\\Desktop\\models\\random_classifier_model")  // give path to save your model

  }
}
