import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object DataProcessing {
  def dataProcessing(spark: SparkSession): DataFrame = {
    val tweetSchema = new StructType()
      .add("id", StringType)
      .add("text", StringType)
      .add(
        "entities",
        new StructType()
          .add("hashtags", ArrayType(new StructType().add("text", StringType)))
      )
      .add("created_at", StringType)
      .add("geo", new StructType().add("coordinates", ArrayType(DoubleType)))
      .add(
        "user",
        new StructType()
          .add("id", StringType)
          .add("name", StringType)
          .add("screen_name", StringType)
          .add("location", StringType)
      )
    import spark.implicits._
    val kafkaDf = KafkaConsumer.startKafkaConsumer(spark)
    val rawDF = kafkaDf
      .select("tweets")
      .select(from_json($"tweets", tweetSchema).as("tweets"))
      .select("tweets.*")

    val transformedDF = rawDF
      .withColumn(
        "hashtags",
        expr("transform(entities.hashtags, h -> h.text)")
      ) // Extract hashtag texts
      .withColumn(
        "created_at",
        when(
          length($"created_at") > 0,
          to_timestamp($"created_at", "EEE MMM dd HH:mm:ss Z yyyy")
        ).otherwise(lit(null).cast(TimestampType))
      )
      .withColumn(
        "geo_coordinates",
        when(
          col("geo.coordinates").isNotNull,
          struct(
            col("geo.coordinates")(0).alias("lat"),
            col("geo.coordinates")(1).alias("lon")
          )
        ).otherwise(
          struct(
            lit(0).cast("double").alias("lat"),
            lit(0).cast("double").alias("lon")
          )
        )
      )
      .withColumn(
        "user",
        struct(
          $"user.id".alias("user_id"),
          $"user.name",
          $"user.screen_name".alias("screen_name"),
          $"user.location"
        )
      )
      .select(
        $"id".alias("tweet_id"),
        $"text",
        $"hashtags",
        $"created_at",
        $"geo_coordinates",
        $"user"
      )

    transformedDF
  }
}
