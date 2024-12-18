import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dataProcessing {
  def dataProcessing(): Unit = {
    val spark = SparkSession.builder()
      .appName("dataProcessor")
      .master("local") // cluster settings (fun info)
      .getOrCreate()

    import spark.implicits._

    val inputPath = "data"
    val rawDF = spark.read
      .json(inputPath)
    rawDF.show()

    val transformedDF = rawDF
      .withColumn("hashtags", expr("transform(entities.hashtags, h -> h.text)")) // Extract hashtag texts
      //      .withColumn("created_at", to_timestamp($"created_at", "E MMM dd HH:mm:ss Z yyyy")) // Convert time format
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

    transformedDF.show(false)

    val outputPath = "output"
    transformedDF.write
      .mode("overwrite")
      .json(outputPath)

    spark.stop()
  }
}
