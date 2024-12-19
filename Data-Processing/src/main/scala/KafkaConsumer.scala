import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.functions.current_timestamp



object KafkaConsumer {
  def startKafkaConsumer(): DataFrame = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val spark = SparkSession.builder()
      .appName("KafkaConsumer")
      .master("local")
      .getOrCreate()



    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "tweets-stream")
      .load()
      .selectExpr("CAST(value AS STRING) as tweets")

    val dataWithTimeStamp = kafkaDF.withColumn("timestamp", current_timestamp())

    dataWithTimeStamp
  }
}