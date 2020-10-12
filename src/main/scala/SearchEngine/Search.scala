package SearchEngine

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Search {
  def main(args: Array[String]): Unit = {
    println("Hello World")
    if (args.length != 1) {
      throw new Exception("Required arguments must be received to start the program: <topic_name>")
    }
    val topicName = args(0)

    /** SPARK STREAMING INITIALIZATION */
    // Initialize StreamingContext object => Main entry point for Spark Streaming functionality.
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SearchCheaperTravelling")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("tmp")

    // Initialize [Kafka Consumer Configuration](http://kafka.apache.org/documentation.html#consumerconfigs)
    val kafkaConsumerParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092"
    )

    /** SPARK STREAMING EXECUTION */
    // Subscribe to topic using second approach => [Direct Stream](https://medium.com/@rinu.gour123/apache-kafka-spark-streaming-integration-af7bd87887fb
    val topicStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConsumerParams, Set(topicName))

    // Start streaming job
    ssc.start()

    // Wait until error occurs or user stops the program and close Cassandra session
    ssc.awaitTermination()
  }

}
