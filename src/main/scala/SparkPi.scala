// scalastyle:off println

import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

case class SensorSegment(sensorId: String, osmStartId: String, osmEndId: String)
case class Telemetry(sensorId: String, timestamp: Long, speedKph: Double)

object SparkPi {
  def main(args: Array[String]) {
    if (args.length != 6) {
      System.err.print("Usage: SparkPi <mongoconn> <mongodb> <mongocoll> <kafkaserver> <kafkatopic> <sensormap>")
      System.exit(1)
    }
    val Array(mongostr, mongodb, mongocoll, kafkaserver, kafkatopic, sensormap) = args

    val spark = SparkSession
      .builder
      .appName("Spark PI")
      .getOrCreate()
    import spark.implicits._

    val linksBySensorId = spark
      .read
      .csv(sensormap)
      .flatMap(row => {
        val sensorId = row.getString(0)
        val osmNodes = row
          .getString(1)
          .split(" ")
          .toList
        osmNodes
          .zip(osmNodes.drop(1))
          .map(link => new SensorSegment(
            sensorId,
            link._1,
            link._2))
      })

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaserver)
      .option("subscribe", kafkatopic)
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .map {
        case (key, value) => {
          val Array(idstr, tsstr, speedstr) = value.split(" ")
          new Telemetry(idstr, tsstr.toLong, speedstr.toDouble)
        }
      }

    val joined = stream
      .joinWith(
        linksBySensorId,
        stream.col("sensorId") === linksBySensorId.col("sensorId"))
      .map {
        case (telemetry, segment) => new Speed(
          segment.osmStartId,
          segment.osmEndId,
          telemetry.timestamp,
          telemetry.speedKph)
      }
    
    val query = joined
      .writeStream
      .outputMode("append")
      .foreach(
        new SpeedUpsertWriter(
          mongostr,
          mongodb,
          mongostr,
          40000))
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
