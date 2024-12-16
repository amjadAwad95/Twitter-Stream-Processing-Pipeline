import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord
}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )
    properties.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](properties)

    val sparkConf =
      new SparkConf().setMaster("local").setAppName("KafkaProducer")

    val sparkContext = new SparkContext(sparkConf)

    val tweetsRDD =
      sparkContext.textFile("data/boulder_flood_geolocated_tweets.json")

    val batchSize = 100
    var tweetCounter = 0
    val topic = "tweets-stream"

    tweetsRDD
      .collect()
      .grouped(batchSize)
      .foreach(batch ⇒ {
        batch.foreach(tweet ⇒ {
          val record =
            new ProducerRecord[String, String](
              topic,
              s"key${tweetCounter}",
              tweet
            )
          producer.send(record)
          tweetCounter += 1
        })
        Thread.sleep(3000)
      })

    producer.close()
  }
}
