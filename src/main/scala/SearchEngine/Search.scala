package SearchEngine

import SearchEngine.Search.TransportType.TransportType
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.log4j.{Level, Logger}
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

    // Hide Logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

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
      record.collect().foreach(inputRow => {
        processTrip(inputRow)
      })
    })

    // Start streaming job
    ssc.start()

    // Wait until error occurs or user stops the program and close Cassandra session
    ssc.awaitTermination()
  }

  private def processTrip(inputRow: (String, String)): Option[RouteGraph] = {
    println(s"Raw ${inputRow._2}")
    val travelQueryInputJson = JSON.parseFull(inputRow._2)

    travelQueryInputJson match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(travelQueryInput: Map[String, Any]) =>
        // Get variables from travel query
        val userId = travelQueryInput("userId").asInstanceOf[Double]
        val src = travelQueryInput("src").asInstanceOf[String]
        val dst = travelQueryInput("dst").asInstanceOf[String]
        val departureDate = travelQueryInput("departureDate").asInstanceOf[String]
        val returnDate = travelQueryInput("returnDate").asInstanceOf[String]

        // Get nearby cities from src and destination [So they can be used as transit stops to make the trip cheaper]
        val srcCities: Set[City] = getCities(src)
        val dstCities: Set[City] = getCities(dst)

        // TODO: Check duplicates in cities or places (I think there are)

        /**
         * Divide found places into three categories:
         * 1. Origin City: City closer to the origin coordinates.
         * 2. Destination Places: City closer to the destination coordinates.
         * 3. Transit Places: The ones that were found near origin or destination but do not meet previous criteria
         */
        val originCity: City = srcCities.find(!_.isTransit).orNull
        val destinationCity: City = dstCities.find(!_.isTransit).orNull
        val transitCities: Set[City] = srcCities.filter(_.isTransit) ++ dstCities.filter(_.isTransit)

        if (originCity != null && destinationCity != null) {
          // Create graph. In its constructor it will use transport APIs to create the edges between the city nodes.
          val route = new RouteGraph(originCity, destinationCity, transitCities, departureDate, returnDate)
          route.createGraph()
          println(route.edges.mkString("Array(", ", ", ")"))

          Some(route)
        }
        else {
          println("No possible route")
          None
        }
      case None =>
        println("Parsing failed")
        None
      case other =>
        println("Unknown data structure: " + other)
        None
    }
  }

  def getCities(coordinates: String): Set[City] = {
    println(s"Fetching cities of ${coordinates}")
    val location = coordinates.split(",").mkString("")
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

    println(s"Processing cities of ${coordinates}")
    val response = JSON.parseFull(nearbyCityRequest.body)
    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(nearbyCitiesQueryRaw: Map[String, List[Map[String, Any]]]) =>
        val nearbyCitiesRaw = nearbyCitiesQueryRaw.getOrElse("data", List())

        // Get information from cities from GeoDB and fill with places retrieved from transport APIs [Sorted by distance]
        val nearbyCities = nearbyCitiesRaw.zipWithIndex.map { case (city, index) =>
          val cityName = city.getOrElse("city", "").toString
          val cityCountryCode = city.getOrElse("countryCode", "").toString
          // val cityDistance = city.getOrElse("distance", Double.MaxValue).asInstanceOf[Double]

          val newCity = City(cityName, cityCountryCode, index != 0, Set()) // isTransit = cityDistance > smallestDistance + thresholdDistance

          // Fill with places from transport APIs
          // TODO: Do async
          newCity.places = newCity.places ++ getPlaces(newCity)
          newCity
        }

        nearbyCities.toSet
      case None =>
        println("Parsing failed")
        Set()
      case other =>
        println("Unknown data structure: " + other)
        Set()
    }
  }

  def getPlaces(city: City): Set[Place] = {
    println(s"Fetching places of ${city.name}")
    // First query airport stations returned by skyscanner
    val skyScannerPlaces = getSkyScannerPlaces(city)

    // Second query airport stations returned by flixbus api
    val flixBusPlaces = getFlixBusPlaces(city)

    skyScannerPlaces ++ flixBusPlaces// ++ flixBusPlaces
  }

  def getSkyScannerPlaces(city: City): Set[Place] = {
    /**
     * Note: Had to execute curl process because of error:
     * "An error in one of the rendering components of OpenRasta prevents the error message from being sent back."
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
        nearbyPlaces.toSet
      case None =>
        println(s"Parsing failed - Request ${request} - Response: ${rawResponse}")
        Set()
      case other =>
        println("Unknown data structure: " + other)
        Set()
    }
  }

  def getFlixBusPlaces(city: City): Set[Place] = {

    val url = s"https://1.flixbus.transport.rest/regions/?query=${city.name.replace(" ", "%20")}"
    val request = Process("curl", Seq("--request", "GET", "--url", url))
    val rawResponse = request.!!(ProcessLogger(_ => null))
    val response = JSON.parseFull(rawResponse)
    println(response)

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(nearbyPlacesQueryRaw: List[Map[String, Any]]) =>
        val nearbyPlaces = nearbyPlacesQueryRaw.map(place => {
          val placeId = place.getOrElse("id", "").toString
          val placeName = place.getOrElse("name", "").toString
          Place(placeId, placeName, TransportType.FlixBus)
        })
        nearbyPlaces.toSet
      case None =>
        println(s"Parsing failed - Request ${request} - Response: ${rawResponse}")
        Set()
      case other =>
        println("Unknown data structure: " + other)
        Set()
    }
  }

  def getRoutes(srcCity: City, destCity: City, departureDate: String, returnDate: String): List[RouteGraphEdge] = {
    var totalRoutes: List[RouteGraphEdge] = List()

    println(srcCity)
    println(destCity)

    // For each pair of places
    srcCity.places.flatMap(srcPlace => {
      destCity.places.map(dstPlace => {
        // If places are of the same type
        if (srcPlace.transportType == dstPlace.transportType) {
          if (srcPlace.transportType == TransportType.SkyScanner) {
            val routes = getSkyScannerRoutes(srcPlace, dstPlace, departureDate, returnDate)
            totalRoutes = totalRoutes ++ routes
          }
        }
      })
    })

    totalRoutes
  }

  def getSkyScannerRoutes(srcPlace: Place, dstPlace: Place, departureDate: String, returnDate: String): List[RouteGraphEdge] = {
    val url = s"https://rapidapi.p.rapidapi.com/apiservices/browseroutes/v1.0/US/EUR/en-US/${srcPlace.id}/${dstPlace.id}/${departureDate}?inboundpartialdate=${returnDate}"
    val request = Process("curl", Seq("--request", "GET", "--url", url, "--header", "x-rapidapi-host:skyscanner-skyscanner-flight-search-v1.p.rapidapi.com", "--header", "x-rapidapi-key:c1d6dfff64msh11b436d0b19f66cp1aa76bjsn9a8b00717a2e"))
    val rawResponse = request.!!(ProcessLogger(_ => null))
    val response = JSON.parseFull(rawResponse)

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(routeQueryResponseRaw: Map[String, List[Map[String, Any]]]) =>
        val routePlaces = routeQueryResponseRaw("Places").asInstanceOf[List[Map[String, Any]]]
        val routeQuotes = routeQueryResponseRaw("Quotes").asInstanceOf[List[Map[String, Any]]]

        // Filter quotes with correct dates (Skyscanner returns alternative days)
        val filteredRouteQuotes = routeQuotes.filter(routeQuote => {
          val outboundLeg = routeQuote("OutboundLeg").asInstanceOf[Map[String, Any]]
          val departureDateRoute = outboundLeg("DepartureDate").asInstanceOf[String]

          // Save if departure date of the route is the same date as target
          departureDateRoute.startsWith(departureDate)
        })

        //
        filteredRouteQuotes.map(routeQuote => {
          // TODO: See if the place is different and create a place otherwise
          RouteGraphEdge(srcPlace, dstPlace, Array(), TransportType.SkyScanner, 1, 1, departureDate, returnDate)
        })
      case None =>
        println(s"Parsing failed - Request ${request} - Response: ${rawResponse}")
        List()
      case other =>
        println("Unknown data structure: " + other)
        List()
    }
  }

  def getFlixBusRoutes(srcPlace: Place, dstPlace: Place, departureDate: String, returnDate: String): List[RouteGraphEdge] = {
    val url = s"https://1.flixbus.transport.rest/journeys/?origin=${srcPlace.id}&destination=${dstPlace.id}&date=${departureDate}"
    val request = Process("curl", Seq("--request", "GET", "--url", url))
    val rawResponse = request.!!(ProcessLogger(_ => null))
    val response = JSON.parseFull(rawResponse)

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(routeQueryResponseRaw: List[Map[String, Any]]) =>
        val routePlaces = routeQueryResponseRaw.map( routePlace => {
          //Get variables
          val departureDateTime = routePlace("departure").asInstanceOf[String]
          val arrivalDateTime = routePlace("arrival").asInstanceOf[String]
          val priceMap = routePlace("price").asInstanceOf[Map[String,Any]]
          val price = priceMap("amount").asInstanceOf[Double]
          (departureDateTime, arrivalDateTime, price, journeyDuration(departureDateTime, arrivalDateTime)) //trip
        })

        routePlaces.map(trip =>
          RouteGraphEdge(srcPlace, dstPlace, Array(), TransportType.FlixBus, trip._3, trip._4, trip._1, returnDate)
        )


      case None =>
        println(s"Parsing failed - Request ${request} - Response: ${rawResponse}")
        List()
      case other =>
        println("Unknown data structure: " + other)
        List()
    }
  }

  // splits the date string in day, time, and some offset (i guess)
  def parseDateTime(dateTime: String) : Array[String] = {

    dateTime.split(Array('T', '.'))

  }

  //calculates the journey duration in minutes
  //assumption: one transit trip is less than 24 hours, otherwise the code needs to be adjusted
  def journeyDuration(departureDateTime: String, arrivalDateTime:String) : Int = {
    val parsedDepartureDate = parseDateTime(departureDateTime) //the incoming string is split into day, time and some offset
    val parsedArrivalDate = parseDateTime(arrivalDateTime)
    val parsedDepartureTime = parsedDepartureDate(1).split(':') //splits the time in hours, minutes and seconds
    val parsedArrivalTime = parsedArrivalDate(1).split(':')

    val hours = if (parsedDepartureTime(0).toInt > parsedArrivalTime(0).toInt) parsedArrivalTime(0).toInt + 24 - parsedDepartureTime(0).toInt else parsedArrivalTime(0).toInt - parsedDepartureTime(0).toInt

    hours * 60 + parsedArrivalTime(1).toInt - parsedDepartureTime(1).toInt


  }

  /**
   * City obtained from GeoDB close to some coordinates.
   *
   * @param name        Name of the city.
   * @param countryCode Universal country code where the city belongs.
   * @param isTransit   The distance of the city from the coordinates is lower than the first found + threshold.
   */
  case class City(name: String, countryCode: String, isTransit: Boolean, var places: Set[Place]) {
    def canEqual(a: Any): Boolean = a.isInstanceOf[City]

    override def equals(that: Any): Boolean = that match {
      case that: Place => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

    override def hashCode(): Int = name.hashCode
  }

  /**
   * Places compatible with an API within a city
   *
   * @param id            Unique id provided by the transport API (SkyScanner or Flixbus)
   * @param name          Name of the city provided by the transport API.
   * @param transportType API used to obtained the place
   */
  case class Place(id: String, name: String, transportType: TransportType) {
    override def equals(that: Any): Boolean = that match {
      case that: Place => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

    def canEqual(a: Any): Boolean = a.isInstanceOf[Place]

    override def hashCode(): Int = id.hashCode
  }

  /**
   * Graph to go from origin to source
   *
   * @param origin      Origin place
   * @param destination Destination Place
   * @param stops       Possible stops
   */
  class RouteGraph(origin: City, destination: City, stops: Set[City], departureDate: String, returnDate: String) {
    var edges: Array[RouteGraphEdge] = Array()
    var nodes: Array[City] = Array()

    def createGraph(): Unit = {
      println("Initializing edges of route graph")
      initializeEdges()
      initializeNodes()
    }

    /**
     * Fill edges graph array
     *
     * @return
     */
    private def initializeEdges(): Unit = {
      // Get possible edges between origin and destination
      val directEdges = getRoutes(origin, destination, departureDate, returnDate)

      // For each stop, get possible edges adding that stop between origin and destination
      val indirectEdgesFromOrigin = stops.flatMap(stop => getRoutes(origin, stop, departureDate, returnDate))
      val indirectEdgesToDestination = stops.flatMap(stop => getRoutes(stop, destination, departureDate, returnDate))

      edges = edges ++ directEdges ++ indirectEdgesFromOrigin ++ indirectEdgesToDestination
    }

    /**
     * Create new nodes from the edges
     */
    private def initializeNodes(): Unit = {
      // First add known nodes
      nodes = nodes :+ origin
      nodes = nodes :+ destination
      nodes = nodes ++ stops

      // For each edge if there is a stop unknown add it as new node
      //      edges.foreach(edge => {
      //      })
    }
  }

  /**
   * Edge of a graph
   *
   * @param origin
   * @param destination
   * @param stops
   * @param transportType
   * @param price
   * @param totalTime
   */
  case class RouteGraphEdge(origin: Place,
                             destination: Place,
                             stops: Array[Place],
                             transportType: TransportType,
                             price: Double,
                             totalTime: Double,
                             departureDateTime: String,
                             returnDateTime: String)

  /**
   * Enumeration indicating the transport APIs used.
   */
  object TransportType extends Enumeration {
    type TransportType = Value
    val SkyScanner, FlixBus = Value
  }

}
