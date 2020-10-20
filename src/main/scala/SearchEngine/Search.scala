package SearchEngine

import SearchEngine.Search.TransportType.TransportType
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalaj.http._

import scala.collection.immutable._
import scala.sys.process.{Process, ProcessLogger}
import scala.util.parsing.json.JSON

object Search {
  val geoDbRadius = 200
  val geoDbLimit = 5
  val geoDbMinPopulation = 100000
  val geoDbSort = "-population"
  val skyScannerCurrency = "EUR"
  val skyScannerLocale = "en-GB"

  val thresholdDistance = 50
  val maxTransitsFlight = 3
  val maxTransitsMixed = 5

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

    // Listen to incoming events of input travel topic
    topicStream.foreachRDD(record => {
      record.collect().foreach((inputRow) => {
        processTrip(inputRow)
      })
    })

    // Start streaming job
    ssc.start()

    // Wait until error occurs or user stops the program and close Cassandra session
    ssc.awaitTermination()
  }

  private def processTrip(inputRow: (String, String)): Unit = {
    println(s"Raw ${inputRow._2}")
    val travelQueryInputJson = JSON.parseFull(inputRow._2)

    travelQueryInputJson match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(travelQueryInput: Map[String, Any]) => {
        // Get variables from travel query
        val src = travelQueryInput.getOrElse("src", "1, 1").toString
        val dst = travelQueryInput.getOrElse("dst", "1, 1").toString

        // Get nearby cities from src and destination [So they can be used as transit stops to make the trip cheaper]
        val srcCities: List[City] = getCities(src)
        val dstCities: List[City] = getCities(dst)

        /**
         * Divide found places into three categories:
         * 1. Origin City: City closer to the origin coordinates.
         * 2. Destination Places: City closer to the destination coordinates.
         * 3. Transit Places: The ones that were found near origin or destination but do not meet previous criteria
         */
        val originCity: City = srcCities.find(!_.isTransit).orNull
        val destinationCity: City = dstCities.find(!_.isTransit).orNull
        val transitCities: List[City] = srcCities.filter(_.isTransit) ++ dstCities.filter(_.isTransit)

        // Create graph. In its constructor it will use transport APIs to create the edges between the city nodes.

      }
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
  }

  def getCities(coordinates: String): List[City] = {
    val nearbyCityRequest = Http(s"https://rapidapi.p.rapidapi.com/v1/geo/locations/${coordinates}/nearbyCities")
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
        val nearbyCitiesRaw = nearbyCitiesQueryRaw.getOrElse("data", List())

        // Get information from cities from GeoDB and fill with places retrieved from transport APIs [Sorted by distance]
        val nearbyCities = nearbyCitiesRaw.zipWithIndex.map{case (city, index) => {
          val cityName = city.getOrElse("city", "").toString
          val cityCountryCode = city.getOrElse("countryCode", "").toString
          // val cityDistance = city.getOrElse("distance", Double.MaxValue).asInstanceOf[Double]

          val newCity = City(cityName, cityCountryCode, index != 0, Set()) // isTransit = cityDistance > smallestDistance + thresholdDistance

          // Fill with places from transport APIs
          newCity.places ++ getPlaces(newCity)
          newCity
        }}

        nearbyCities
      case None =>
        println("Parsing failed")
        List()
      case other =>
        println("Unknown data structure: " + other)
        List()
    }
  }


  def getPlaces(city: City): List[Place] = {
    // First query airport stations returned by skyscanner
    val skyScannerPlaces = getSkyScannerPlaces(city)

    // Second query airport stations returned by flixbus api
    // TODO

    skyScannerPlaces // ++ flixBusPlaces
  }

  def getSkyScannerPlaces(city: City): List[Place] = {
    /**
    * Note: Had to execute curl process because of error:
    *  "An error in one of the rendering components of OpenRasta prevents the error message from being sent back."
    */
    val url = s"https://rapidapi.p.rapidapi.com/apiservices/autosuggest/v1.0/${city.countryCode}/EUR/en-GB/?query=${city.name.replace(" ", "%20")}"
    val request = Process("curl", Seq("--request", "GET", "--url", url, "--header", "x-rapidapi-host:skyscanner-skyscanner-flight-search-v1.p.rapidapi.com", "--header", "x-rapidapi-key:c1d6dfff64msh11b436d0b19f66cp1aa76bjsn9a8b00717a2e"))
    val rawResponse = request.!!(ProcessLogger(_ => null))
    val response = JSON.parseFull(rawResponse)

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(nearbyPlacesQueryRaw: Map[String, List[Map[String, Any]]]) =>
        val nearbyPlacesRaw = nearbyPlacesQueryRaw.getOrElse("Places", List())
        val nearbyPlaces = nearbyPlacesRaw.map(place => {
          val placeId = place.getOrElse("PlaceId", "").toString
          val placeName = place.getOrElse("PlaceName", "").toString
          Place(placeId, placeName, TransportType.SkyScanner)
        })
        nearbyPlaces
      case None =>
        println(s"Parsing failed - Request ${request} - Response: ${rawResponse}")
        List()
      case other =>
        println("Unknown data structure: " + other)
        List()
    }
  }

  /**
   * City obtained from GeoDB close to some coordinates.
   * @param name Name of the city.
   * @param countryCode Universal country code where the city belongs.
   * @param isTransit The distance of the city from the coordinates is lower than the first found + threshold.
   */
  case class City(name: String, countryCode: String, isTransit: Boolean, places: Set[Place]) {
    def canEqual(a: Any): Boolean = a.isInstanceOf[City]

    override def equals(that: Any): Boolean = that match {
      case that: Place => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

    override def hashCode(): Int = name.hashCode
  }

  object TransportType extends Enumeration {
    type TransportType = Value
    val SkyScanner, FlixBus = Value
  }

  /**
   * Places compatible with an API within a city
   * @param id Unique id provided by the transport API (SkyScanner or Flixbus)
   * @param name Name of the city provided by the transport API.
   * @param transportType API used to obtained the place
   */
  case class Place(id: String, name: String, transportType: TransportType) {
    def canEqual(a: Any): Boolean = a.isInstanceOf[Place]

    override def equals(that: Any): Boolean = that match {
      case that: Place => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

    override def hashCode(): Int = id.hashCode
  }

  /**
   * Graph to go from origin to source
   * @param origin Origin place
   * @param destination Destination Place
   * @param stops Possible stops
   */
  class RouteGraph(origin: City, destination: City, stops: Array[City]) {
      def getRoutesBetweenCities(srcCity: City, destCity: City): Unit = {

      }
//    val edges = City ? Place?
      def createGraph(): Unit = {
        // Get possible edges between city and destination
      }
  }
}
