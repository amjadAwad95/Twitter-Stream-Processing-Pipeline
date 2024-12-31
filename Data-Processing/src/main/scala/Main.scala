import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val spark = SparkSession
      .builder()
      .appName("Twitter-Stream-Processing-Pipeline")
      .config("spark.es.nodes", "localhost")
      .config("spark.es.port", "9200")
      .config("spark.es.net.http.auth.user", "elastic")
      .config("spark.es.net.http.auth.pass", "951741")
      .config("spark.es.net.ssl", "true")
      .config(
        "spark.es.net.sll.cert",
        "C:\\elasticsearch\\elasticsearch-8.16.1\\config\\certs\\http_ca.crt"
      )
      .config("spark.es.nodes.wan.only", "true")
      .master("local")
      .getOrCreate()

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    val df = DataProcessing.dataProcessing(spark)
    val dfWithSentiments = SentimentAnalysis.getSentiments(df)
    //KafkaConsumer.writeStream(dfWithSentiments)
    KafkaConsumer.writeStreamElasticSearch(dfWithSentiments)
  }
}
