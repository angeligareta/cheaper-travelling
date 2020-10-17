package SearchEngine

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

  private def processTrip(inputRow: (String, String)) = {
    println(s"Raw ${inputRow._2}")
    val travelQueryInputJson = JSON.parseFull(inputRow._2)

    travelQueryInputJson match {
      // Matches if jsonStr is valid JSON and represents a Map of Strings to Any
      case Some(travelQueryInput: Map[String, Any]) => {
        val src = travelQueryInput.getOrElse("src", "1, 1").toString
        val dst = travelQueryInput.getOrElse("dst", "1, 1").toString

        // Get nearby places
        val srcNearbyPlaces: Set[Place] = getNearbyPlacesFromCityRaw(src)
        val dstNearbyPlaces: Set[Place] = getNearbyPlacesFromCityRaw(dst)

        val srcPlaces: Set[Place] = srcNearbyPlaces.filter(!_.isTransit)
        val dstPlaces: Set[Place] = dstNearbyPlaces.filter(!_.isTransit)
        val transitPlaces: Set[Place] = srcNearbyPlaces.filter(_.isTransit) ++ dstNearbyPlaces.filter(_.isTransit)

        // Get possibles routes from each src to each dst
        var possiblePlacesCombinations: Array[Array[Place]] = Array()
        srcPlaces.foreach(srcPlace => {
          dstPlaces.foreach(dstPlace => {
            transitPlaces.foreach(transitPlace => {
              possiblePlacesCombinations = possiblePlacesCombinations :+ Array(srcPlace, transitPlace, dstPlace)
            })

            // Also direct combinations
            possiblePlacesCombinations = possiblePlacesCombinations :+ Array(srcPlace, dstPlace)
          })
        })

        println("Possible combinations")
        possiblePlacesCombinations.foreach(possiblePlacesCombination => {
          println(possiblePlacesCombination.mkString("Array(", ", ", ")"))
        })

        // For each combination look for routes
        // Flights

        // TODO: Transport
      }
      case None => println("Parsing failed")
      case other => println("Unknown data structure: " + other)
    }
  }

  private def getNearbyPlacesFromCityRaw (city: String): Set[Place] = {
    var srcNearbyPlaces: Set[Place] = Set()

    val srcNearbyCities = getNearbyPlacesFromCity(city)
    srcNearbyCities.foreach(city => {
      val srcNearbySkyPlaces = getNearbyStations(city)
      // println(s"Places close to city ${city}: ${srcNearbySkyPlaces}")
      srcNearbySkyPlaces.foreach(place => {
        srcNearbyPlaces += place
      })
    })

    srcNearbyPlaces
  }

  def getNearbyPlacesFromCity(location: String): List[City] = {
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
        val smallestDistance = if (nearbyCities.nonEmpty) nearbyCities.head.getOrElse("distance", Double.MaxValue).asInstanceOf[Double] else 0.0

        val nearbyCitiesNames = nearbyCities.map(city => {
          val cityName = city.getOrElse("city", "").toString
          val cityCountryCode = city.getOrElse("countryCode", "").toString
          val cityDistance = city.getOrElse("distance", Double.MaxValue).asInstanceOf[Double]

          City(cityName, cityCountryCode, isTransit = (cityDistance > smallestDistance + thresholdDistance))
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


  def getNearbyStations(city: City): List[Place] = {
    //    val nearbyCityRequest = Http(s"https://rapidapi.p.rapidapi.com/apiservices/autosuggest/v1.0/${city.countryCode}/EUR/en-GB/")
    //      .param("query", city.name)
    //      .header("x-rapidapi-host", "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com")
    //      .header("x-rapidapi-key", "c1d6dfff64msh11b436d0b19f66cp1aa76bjsn9a8b00717a2e")
    //      .asString
    //    println(nearbyCityRequest)

    // Had to execute curl process because of error "An error in one of the rendering components of OpenRasta prevents the error message from being sent back."
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
          Place(placeId, placeName, city.isTransit)
        })
        nearbyPlaces
      case None =>
        println(request)
        println(rawResponse)
        println("Parsing failed")
        List()
      case other =>
        println("Unknown data structure: " + other)
        List()
    }
  }

  case class City(name: String, countryCode: String, isTransit: Boolean ) {
    def canEqual(a: Any): Boolean = a.isInstanceOf[City]

    override def equals(that: Any): Boolean = that match {
      case that: Place => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

    override def hashCode(): Int = {
      name.hashCode
    }
  }

  case class Place(id: String, name: String, isTransit: Boolean) {
    override def equals(that: Any): Boolean = that match {
      case that: Place => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

    def canEqual(a: Any): Boolean = a.isInstanceOf[Place]

    override def hashCode(): Int = {
      id.hashCode
    }
  }

}
