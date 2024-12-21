import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object DataProcessing {
  def dataProcessing(): DataFrame = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    val spark = SparkSession.builder()
      .appName("dataProcessor")
      .master("local") // cluster settings (fun info)
      .getOrCreate()

    val tweetSchema = new StructType()
      .add("id", StringType)
      .add("text", StringType)
      .add("entities", new StructType().add("hashtags", ArrayType(new StructType().add("text", StringType))))
      .add("created_at", StringType)
      .add("geo", new StructType().add("coordinates", ArrayType(DoubleType)))
      .add("user", new StructType()
        .add("id", StringType)
        .add("name", StringType)
        .add("screen_name", StringType)
        .add("location", StringType))
    import spark.implicits._
    val kafkaDf = KafkaConsumer.startKafkaConsumer()
    val rawDF = kafkaDf.select("tweets")
      .select(from_json($"tweets", tweetSchema).as("tweets"))
      .select("tweets.*")



    val transformedDF = rawDF
      .withColumn("hashtags", expr("transform(entities.hashtags, h -> h.text)")) // Extract hashtag texts
      .withColumn("geo_coordinates", struct(
        $"geo.coordinates"(0).alias("lat"),
        $"geo.coordinates"(1).alias("lon")
      ))
      .withColumn("user", struct(
        $"user.id",
        $"user.name",
        $"user.screen_name".alias("username"),
        $"user.location"
      ))
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
