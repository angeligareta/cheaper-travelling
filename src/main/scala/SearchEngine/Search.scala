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

  val skyScannerKeys: Array[String] = Array("7863185a61msh6de35a511c1c796p1f805djsn956c714b6324", "c1d6dfff64msh11b436d0b19f66cp1aa76bjsn9a8b00717a2e", "023f9d23b4msh7473f3840e44c05p19f165jsn5dcc120f3285", "4cdbce5e3cmsh89111bd2e4e842fp193f46jsnf95a75c11f60", "16f98fa46emshb2cecc0f4e4d02cp13f5bajsn7fad3fee738f")
  var keyIndex = 0

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
        //val returnDate = travelQueryInput("returnDate").asInstanceOf[String]

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

        println(s"Origin city: ${originCity}")
        println(s"Dst city: ${destinationCity}")
        println(s"Transity cities: ${transitCities}")

        if (originCity != null && destinationCity != null) {
          // Create graph. In its constructor it will use transport APIs to create the edges between the city nodes.
          val route = new RouteGraph(originCity, destinationCity, transitCities, departureDate)
          route.createGraph()

          println("***Graph***")
          println("**Edges**")
          route.edges.foreach(edge => {
            println(s"(${edge.origin.id}:${edge.origin.name}:${edge.origin.transportType}:${edge.origin.cityName}) ==(${edge.price}:${edge.totalTime})=> (${edge.destination.id}:${edge.destination.name}:${edge.destination.transportType}:${edge.destination.cityName})")
          })
          println("**Nodes**")
          route.nodes.foreach(println)

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
          val cityName = city("city").asInstanceOf[String]
          val cityCountryCode = city("countryCode").asInstanceOf[String]
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

    skyScannerPlaces ++ flixBusPlaces // ++ flixBusPlaces
  }

  def getSkyScannerPlaces(city: City): Set[Place] = {
    /**
     * Note: Had to execute curl process because of error:
     * "An error in one of the rendering components of OpenRasta prevents the error message from being sent back."
     */
    keyIndex += 1
    val url = s"https://rapidapi.p.rapidapi.com/apiservices/autosuggest/v1.0/${city.countryCode}/EUR/en-GB/?query=${city.name.replace(" ", "%20")}"
    val request = Process("curl", Seq("--request", "GET", "--url", url, "--header", "x-rapidapi-host:skyscanner-skyscanner-flight-search-v1.p.rapidapi.com", "--header", s"x-rapidapi-key:${skyScannerKeys(keyIndex % skyScannerKeys.length)}"))
    val rawResponse = request.!!(ProcessLogger(_ => null))
    val response = JSON.parseFull(rawResponse)

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(nearbyPlacesQueryRaw: Map[String, List[Map[String, Any]]]) =>
        val nearbyPlacesRaw = nearbyPlacesQueryRaw.getOrElse("Places", List())

        val nearbyPlaces = nearbyPlacesRaw.map(place => {
          val placeId = place("PlaceId").asInstanceOf[String]
          val placeName = place("PlaceName").asInstanceOf[String]
          Place(placeId, placeName, TransportType.SkyScanner, city.name)
        })

        // Remove generic groups [As there would be duplicates] These are the ones with format: XXXX-sky
        nearbyPlaces.filter(nearbyPlace => nearbyPlace.id.split("-")(0).length != 4).toSet
      case None =>
        println(s"Parsing failed - Request ${request} - Response: ${rawResponse}")
        Set()
      case other =>
        println("Unknown data structure: " + other)
        Set()
    }
  }

  def getFlixBusPlaces(city: City): Set[Place] = {
    // TODO: Change to normal request
    val url = s"https://1.flixbus.transport.rest/regions/?query=${city.name.replace(" ", "%20")}"
    val request = Process("curl", Seq("--request", "GET", "--url", url))
    val rawResponse = request.!!(ProcessLogger(_ => null))
    val response = JSON.parseFull(rawResponse)

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      // Format: List(Map(weight -> 1.0, name -> Stockholm, score -> 4.000819896, id -> 3798, relevance -> 4.000819896, type -> region))
      case Some(nearbyPlacesQueryRaw: List[Map[String, Any]]) =>
        val nearbyPlaces = nearbyPlacesQueryRaw.map(place => {
          val placeId = place("id").asInstanceOf[String]
          val placeName = place("name").asInstanceOf[String]
          Place(placeId, placeName, TransportType.FlixBus, city.name)
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

  def getRoutes(srcCity: City, dstCity: City, departureDate: String): List[RouteGraphEdge] = {
    var totalRoutes: List[RouteGraphEdge] = List()

    // For each pair of places
    srcCity.places.flatMap(srcPlace => {
      dstCity.places.map(dstPlace => {
        // If places are of the same type
        if (srcPlace.transportType == dstPlace.transportType) {
          if (srcPlace.transportType == TransportType.SkyScanner) {
            totalRoutes = totalRoutes ++
              getSkyScannerRoutes(srcPlace, dstPlace, departureDate, srcCity, dstCity)
          }
          else if (srcPlace.transportType == TransportType.FlixBus){
            totalRoutes = totalRoutes ++
              getFlixBusRoutes(srcPlace, dstPlace, departureDate, srcCity, dstCity)
          }
        }
      })
    })

    totalRoutes
  }

  /**
   * Returns place name that has the id origin Id in route places skyscanner journey response
   * Format: List(Map(Name -> Stockholm Bromma, CityName -> Stockholm, CountryName -> Sweden, CityId -> STOC, PlaceId -> 42881.0, SkyscannerCode -> BMA, IataCode -> BMA, Type -> Station)
   * @param placeId
   * @param routePlaces
   * @return
   */
  def getSkyScannerPlaceName(placeId: String, routePlaces: List[Map[String, Any]]): String = {
    val routePlace = routePlaces.find(routePlace => {
      val id = routePlace("PlaceId").asInstanceOf[Double].toString
      id == placeId
    })

    routePlace.get("Name").asInstanceOf[String]
  }

  def getSkyScannerRoutes(srcPlace: Place, dstPlace: Place, departureDate: String, srcCity: City, dstCity: City): List[RouteGraphEdge] = {
    keyIndex += 1
    val url = s"https://rapidapi.p.rapidapi.com/apiservices/browseroutes/v1.0/${srcCity.countryCode}/EUR/en-US/${srcPlace.id}/${dstPlace.id}/${departureDate}"
    val request = Process("curl", Seq("--request", "GET", "--url", url, "--header", "x-rapidapi-host:skyscanner-skyscanner-flight-search-v1.p.rapidapi.com", "--header", s"x-rapidapi-key:${skyScannerKeys(keyIndex % skyScannerKeys.length)}"))
    val rawResponse = request.!!(ProcessLogger(_ => null))
    val response = JSON.parseFull(rawResponse)

//    println(srcPlace)
//    println(dstPlace)
//    println(response)

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(routeQueryResponseRaw: Map[String, List[Map[String, Any]]]) =>
        // Free plan
        if (routeQueryResponseRaw.contains("message")) {
          println("Omitting due to rate limit...")
          List()
        }
        else {
          val routePlaces = routeQueryResponseRaw("Places").asInstanceOf[List[Map[String, Any]]]
          val routeQuotes = routeQueryResponseRaw("Quotes").asInstanceOf[List[Map[String, Any]]]

          // Filter quotes with correct dates (Skyscanner returns alternative days)
          // Format: Quotes -> List(Map(OutboundLeg -> Map(CarrierIds -> List(1090.0), OriginId -> 89268.0, DestinationId -> 56332.0, DepartureDate -> 2020-12-04T00:00:00), QuoteId -> 1.0, Direct -> false, MinPrice -> 118.0, QuoteDateTime -> 2020-10-21T10:15:00))
          val filteredRouteQuotes = routeQuotes.filter(routeQuote => {
            val outboundLeg = routeQuote("OutboundLeg").asInstanceOf[Map[String, Any]]
            val departureDateRoute = outboundLeg("DepartureDate").asInstanceOf[String]
            // Save if departure date of the route is the same date as target
            departureDateRoute.startsWith(departureDate)
          })

          // Map the route quotes to RouteGraphEdge
          filteredRouteQuotes.map(routeQuote => {
            println(s"Skyscanner route: ${routeQuote}")
            val outboundLegMap = routeQuote("OutboundLeg").asInstanceOf[Map[String, Any]]
            val originId = outboundLegMap("OriginId").asInstanceOf[Double].toString
            val destinationId = outboundLegMap("DestinationId").asInstanceOf[Double].toString

            val isDirect = routeQuote("Direct").asInstanceOf[Boolean]
            val minPrice = routeQuote("MinPrice").asInstanceOf[Double]

            // Create new place from origin and destination for indetifiend unique skyscanner airports
            val newSrcPlace = Place(originId, getSkyScannerPlaceName(originId, routePlaces), TransportType.SkyScanner, srcCity.name)
            val newDstPlace = Place(destinationId, getSkyScannerPlaceName(destinationId, routePlaces), TransportType.SkyScanner, dstCity.name)

            RouteGraphEdge(newSrcPlace, newDstPlace, isDirect, TransportType.SkyScanner, minPrice, 0, departureDate, departureDate)
          })
        }
      case None =>
        println(s"Parsing failed - Request ${request} - Response: ${rawResponse}")
        List()
      case other =>
        println("Unknown data structure: " + other)
        List()
    }
  }

  def getFlixBusRoutes(srcPlace: Place, dstPlace: Place, departureDate: String, srcCity: City, dstCity: City): List[RouteGraphEdge] = {
    // TODO: Change to normal API
    val url = s"https://1.flixbus.transport.rest/journeys/?origin=${srcPlace.id}&destination=${dstPlace.id}&date=${departureDate}"
    val request = Process("curl", Seq("--request", "GET", "--url", url))
    val rawResponse = request.!!(ProcessLogger(_ => null))
    val response = JSON.parseFull(rawResponse)

//    println(srcPlace)
//    println(dstPlace)

    response match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(routeQueryResponseRaw: List[Map[String, Any]]) =>
//        println(routeQueryResponseRaw)
        val routePlaces: List[RouteGraphEdge] = routeQueryResponseRaw.map(routeQuote => {
          println(s"Flixbus route: ${routeQuote}")
          // Get variables
          val origin = routeQuote("origin").asInstanceOf[Map[String, Any]] // Format: Map(type -> station, id -> 13628, name -> Arlanda Airport T5, importance -> 100.0) // destination -> Map(type -> station, id -> 13828, name -> Uppsala, importance -> null)
          val destination = routeQuote("destination").asInstanceOf[Map[String, Any]] //  Format: Map(type -> station, id -> 28998, name -> Uppsala Business Park, importance -> null)
          val departureDateTime = routeQuote("departure").asInstanceOf[String] // Departure time format: 2020-12-08T12:45:00.000Z
          val arrivalDateTime = routeQuote("arrival").asInstanceOf[String]
          val isDirect = routeQuote("direct").asInstanceOf[Boolean]
          val priceMap = routeQuote("price").asInstanceOf[Map[String, Any]]
          val price = priceMap("amount").asInstanceOf[Double]

          val totalTime = journeyDuration(departureDateTime, arrivalDateTime)

          // Create new place from origin and destination to represent unique flixbus stations
          val newSrcPlace = Place(origin("id").asInstanceOf[String], origin("name").asInstanceOf[String], TransportType.FlixBus, srcCity.name)
          val newDstPlace = Place(destination("id").asInstanceOf[String], destination("name").asInstanceOf[String], TransportType.FlixBus, dstCity.name)

          // TODO: Check if there are available slots ? available -> Map(seats -> 999.0, slots -> 999.0)
          // TODO: Add note in graph edge like "2 places left"
          RouteGraphEdge(newSrcPlace, newDstPlace, isDirect, TransportType.FlixBus, price, totalTime, departureDateTime, arrivalDateTime)
        })
        routePlaces
      case None =>
        println(s"Parsing failed - Request ${request} - Response: ${rawResponse}")
        List()
      case other =>
        println("Unknown data structure: " + other)
        List()
    }
  }

  /**
   * Calculates the journey duration in minutes
   * Assumption: one transit trip is less than 24 hours, otherwise the code needs to be adjusted
    */
  def journeyDuration(departureDateTime: String, arrivalDateTime: String): Int = {
    val parsedDepartureDate = parseDateTime(departureDateTime) //the incoming string is split into day, time and some offset
    val parsedArrivalDate = parseDateTime(arrivalDateTime)
    val parsedDepartureTime = parsedDepartureDate(1).split(':') //splits the time in hours, minutes and seconds
    val parsedArrivalTime = parsedArrivalDate(1).split(':')

    val hours = if (parsedDepartureTime(0).toInt > parsedArrivalTime(0).toInt) parsedArrivalTime(0).toInt + 24 - parsedDepartureTime(0).toInt else parsedArrivalTime(0).toInt - parsedDepartureTime(0).toInt

    hours * 60 + parsedArrivalTime(1).toInt - parsedDepartureTime(1).toInt
  }

  /**
   * Splits the date string in day, time, and offset
    */
  def parseDateTime(dateTime: String): Array[String] = {
    dateTime.split(Array('T', '.'))
  }

  /**
   * City obtained from GeoDB close to some coordinates.
   *
   * @param name        Name of the city.
   * @param countryCode Universal country code where the city belongs.
   * @param isTransit   The distance of the city from the coordinates is lower than the first found + threshold.
   */
  case class City(name: String, countryCode: String, isTransit: Boolean, var places: Set[Place]) {
    override def equals(that: Any): Boolean = that match {
      case that: City => this.name == that.name
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
  case class Place(id: String, name: String, transportType: TransportType, cityName: String) {
    override def equals(that: Any): Boolean = that match {
      case that: Place => this.id == that.id
      case _ => false
    }

    override def hashCode(): Int = id.hashCode
  }

  /**
   * Graph to go from origin to source
   *
   * @param origin      Origin place
   * @param destination Destination Place
   * @param stops       Possible stops
   */
  class RouteGraph(origin: City, destination: City, stops: Set[City], departureDate: String) {
    var edges: Array[RouteGraphEdge] = Array()
    var nodes: Set[City] = Set()

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
      val directEdges = getRoutes(origin, destination, departureDate)

      // For each stop, get possible edges adding that stop between origin and destination
      val indirectEdgesFromOrigin = stops.flatMap(stop => getRoutes(origin, stop, departureDate))
      val indirectEdgesToDestination = stops.flatMap(stop => getRoutes(stop, destination, departureDate))

      edges = edges ++ directEdges ++ indirectEdgesFromOrigin ++ indirectEdgesToDestination

      // TODO: Filter edges by total price and duration
    }

    /**
     * Create new nodes from the edges
     */
    private def initializeNodes(): Unit = {
      // Create city groups
      edges.foreach(edge => {
        nodes += City(edge.origin.cityName, "", edge.origin.cityName == origin.name, Set())
        nodes += City(edge.destination.cityName, "", edge.destination.cityName == destination.name, Set())
      })

      // Fill nodes places
      edges.foreach(edge => {
        nodes = nodes.map(node => {
          var newNode = node
          // Add origin
          if (node.name == edge.origin.cityName) {
            newNode.places = newNode.places + edge.origin
          }
          // Add destination
          if (node.name == edge.destination.cityName) {
            newNode.places = newNode.places + edge.destination
          }
          newNode
        })
      })
    }
  }

  /**
   * Edge of a graph
   *
   * @param origin
   * @param destination
   * @param isDirect
   * @param transportType
   * @param price
   * @param totalTime
   */
  case class RouteGraphEdge(origin: Place,
                            destination: Place,
                            //stops: Array[Place],
                            isDirect: Boolean,
                            transportType: TransportType,
                            price: Double,
                            totalTime: Double,
                            departureDateTime: String,
                            arrivalDateTime: String)

  /**
   * Enumeration indicating the transport APIs used.
   */
  object TransportType extends Enumeration {
    type TransportType = Value
    val SkyScanner, FlixBus = Value
  }

}
