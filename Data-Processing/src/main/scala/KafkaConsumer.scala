import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.Row

object KafkaConsumer {
  def startKafkaConsumer(spark: SparkSession): DataFrame = {
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "tweets-stream")
      .load()
      .selectExpr("CAST(value AS STRING) as tweets")

    val dataWithTimeStamp = kafkaDF.withColumn("timestamp", current_timestamp())

    dataWithTimeStamp
  }
  def writeStream(df: DataFrame): Unit = {
    val query = df.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()
    query.awaitTermination()
  }

  def writeStreamElasticSearch(df: DataFrame): Unit = {
    df.writeStream
      .foreachBatch { (batchDF: Dataset[Row], batchId: Long) ⇒
        {
          batchDF.write
            .format("org.elasticsearch.spark.sql")
            .option("checkpointLocation", "checkpoint1")
            .option("es.resource", "tweets")
            .mode("append")
            .save()
        }
      }
      .option("checkpointLocation", "checkpoint1")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
