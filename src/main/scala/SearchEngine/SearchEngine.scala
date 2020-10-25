package SearchEngine

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.Set
import scala.io.StdIn._
import scala.util.Random

object SearchEngine {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      throw new Exception("Required arguments must be received to start the program: <topic_name>")
    }
    val topicName = args(0)

    // Hide Logs
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    // Initialize [Kafka Consumer Configuration](http://kafka.apache.org/documentation.html#consumerconfigs)
    val kafkaProducerParams = new Properties()
    kafkaProducerParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    kafkaProducerParams.put(ProducerConfig.CLIENT_ID_CONFIG, "Generator")
    kafkaProducerParams.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProducerParams.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](kafkaProducerParams)

    /**
     * Collect data
     */
    val userId = Random.nextInt(Integer.MAX_VALUE)
    println(s"Welcome to CheapTravelling Search Engine! You are the user ${userId}")
    println("\tEnter source coordinates in the ISO-6709 format: ±DD.DDDD,±DDD.DDDD. Example: +59.3493243,+18.0707407")
    val src = readLine()
    println("\tEnter destination coordinates in the ISO-6709 format: ±DD.DDDD,±DDD.DDDD. Example: +55.6761,+12.5683")
    val dst = readLine()
    println("\tEnter desired departure date in the format: yyyy-mm-dd. Example: 2020-10-29")
    val departureDate = readLine()
    println("\tEnter price range in the format: min,max. Example: 100,200")
    val priceRange = readLine()
    println("\tEnter time duration range in hours in the format: min,max. Example: 2,5")
    val timeTravelRange = readLine()

    val tripRaw = s"""{"userId": ${userId}, "src": "${src}","departureDate":"${departureDate}","dst":"${dst}", "priceRange": ${priceRange.split(",").mkString("[", ",", "]")},"timeTravelRange": ${timeTravelRange.split(",").mkString("[", ",", "]")}}"""
    //    val tripRaw = """{"userId":123,"src":"+59.3493243,+18.0707407","departureDate":"2020-10-29","dst":"+55.6761,+12.5683", "priceRange":[100,200],"timeTravelRange":[2,5]}""".stripMargin

    // Send data to Kafka
    val data = new ProducerRecord[String, String](topicName, s"input-${userId}", tripRaw)
    producer.send(data)
    print(s"Sending to Kafka: ${data}\n")

    // Wait until error occurs or user stops the program and close Cassandra session
    producer.close()

    /**
     * Spark Streaming
     */
    val kafkaConsumerParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
    )

    // Initialize StreamingContext object => Main entry point for Spark Streaming functionality.
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CheaperTravelling_SearchEngine")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    //    ssc.checkpoint("tmp-search-engine")

    val topicStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConsumerParams, Set(topicName))

    // Listen to incoming events of input travel topic
    topicStream.foreachRDD(record => {
      record.collect().foreach(inputRow => {
        if (inputRow._1 == s"output-${userId}") {
          println(s"Kafka result for user ${userId} query")
          println(inputRow._2)
        }
      })
    })

    // Start streaming job
    ssc.start()

    // Wait until error occurs or user stops the program and close Cassandra session
    ssc.awaitTerminationOrTimeout(60000)
  }
}
