package SearchEngine

import java.net.URLEncoder

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalaj.http._

import scala.sys.process.{Process, ProcessLogger}
import scala.util.parsing.json.JSON

object Search {
  val geoDbRadius = 200
  val geoDbLimit = 5
  val geoDbMinPopulation = 100000
  val geoDbSort = "-population"
  val skyScannerCurrency = "EUR"
  val skyScannerLocale = "en-GB"

  def main(args: Array[String]): Unit = {
    //    if (args.length != 1) {
    //      throw new Exception("Required arguments must be received to start the program: <topic_name>")
    //    }
    val topicName = "input-travel"

    /** SPARK STREAMING INITIALIZATION */
    // Initialize StreamingContext object => Main entry point for Spark Streaming functionality.
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SearchCheaperTravelling")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("tmp")

    // Initialize [Kafka Consumer Configuration](http://kafka.apache.org/documentation.html#consumerconfigs)
    val kafkaConsumerParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
    )

    /** SPARK STREAMING EXECUTION */
    // Subscribe to topic using second approach => [Direct Stream](https://medium.com/@rinu.gour123/apache-kafka-spark-streaming-integration-af7bd87887fb
    val topicStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConsumerParams, Set(topicName))

    topicStream.foreachRDD(record => {
      record.collect().foreach((inputRow) => {
        println(s"Raw ${inputRow._2}")
        val travelQueryInputJson = JSON.parseFull(inputRow._2)

        travelQueryInputJson match {
          // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
          case Some(travelQueryInput: Map[String, Any]) => {
            val src = travelQueryInput.getOrElse("src", "1, 1").toString
            val dst = travelQueryInput.getOrElse("dst", "1, 1").toString

            println("***SRC***")
            val srcNearbyCities = getNearbyCities(src)
            srcNearbyCities.foreach(city => {
              val srcNearbySkyPlaces = getNearbyStations(city)
              println(s"Places close to city ${city}: ${srcNearbySkyPlaces}")
            })

            println("***DST***")
            val dstNearbyCities = getNearbyCities(dst)
            dstNearbyCities.foreach(city => {
              val dstNearbySkyPlaces = getNearbyStations(city)
              println(s"Places close to city ${city}: ${dstNearbySkyPlaces}")
            })
          }
          case None => println("Parsing failed")
          case other => println("Unknown data structure: " + other)
        }
      })
    })


    // Start streaming job
    ssc.start()

    // Wait until error occurs or user stops the program and close Cassandra session
    ssc.awaitTermination()
  }

  def getNearbyCities(location: String): List[City] = {
    val nearbyCityRequest = Http(s"https://rapidapi.p.rapidapi.com/v1/geo/locations/${location}/nearbyCities")
      .param("radius", geoDbRadius.toString)
      .param("limit", geoDbLimit.toString)
      .param("minPopulation", geoDbMinPopulation.toString)
      .param("sort", geoDbSort)
      .param("distanceUnit", "KM")
      .param("types", "CITY")
      .header("x-rapidapi-host", "wft-geo-db.p.rapidapi.com")
      .header("x-rapidapi-key", "c1d6dfff64msh11b436d0b19f66cp1aa76bjsn9a8b00717a2e")
      .header("useQueryString", "true")
      .asString

    val response = JSON.parseFull(nearbyCityRequest.body)

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(nearbyCitiesQueryRaw: Map[String, List[Map[String, Any]]]) =>
        val nearbyCities = nearbyCitiesQueryRaw.getOrElse("data", List())
        val nearbyCitiesNames = nearbyCities.map(city => {
          val cityName = city.getOrElse("city", "").toString
          val cityCountryCode = city.getOrElse("countryCode", "").toString
          City(cityName, cityCountryCode)
        })
        nearbyCitiesNames
      case None =>
        println("Parsing failed")
        List()
      case other =>
        println("Unknown data structure: " + other)
        List()
    }
  }


  def getNearbyStations(city: City) = {
//    val nearbyCityRequest = Http(s"https://rapidapi.p.rapidapi.com/apiservices/autosuggest/v1.0/${city.countryCode}/EUR/en-GB/")
//      .param("query", city.name)
//      .header("x-rapidapi-host", "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com")
//      .header("x-rapidapi-key", "c1d6dfff64msh11b436d0b19f66cp1aa76bjsn9a8b00717a2e")
//      .asString
//    println(nearbyCityRequest)

    // Had to execute curl process because of error "An error in one of the rendering components of OpenRasta prevents the error message from being sent back."
    val url = s"https://rapidapi.p.rapidapi.com/apiservices/autosuggest/v1.0/${city.countryCode}/EUR/en-GB/?query=${city.name}"
    val request = Process("curl", Seq("--request", "GET", "--url", url, "--header", "x-rapidapi-host:skyscanner-skyscanner-flight-search-v1.p.rapidapi.com", "--header", "x-rapidapi-key:c1d6dfff64msh11b436d0b19f66cp1aa76bjsn9a8b00717a2e"))
    val response = JSON.parseFull(request.!!(ProcessLogger(_ => null)))

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(nearbyPlacesQueryRaw: Map[String, List[Map[String, Any]]]) =>
        val nearbyPlacesRaw = nearbyPlacesQueryRaw.getOrElse("Places", List())
        val nearbyPlaces = nearbyPlacesRaw.map(place => {
          val placeId = place.getOrElse("PlaceId", "").toString
          val placeName = place.getOrElse("PlaceName", "").toString
          Place(placeId, placeName)
        })
        nearbyPlaces
      case None =>
        println("Parsing failed")
        List()
      case other =>
        println("Unknown data structure: " + other)
        List()
    }
  }

  case class City(name: String, countryCode: String)
  case class Place(id: String, name: String)
}
