object Main {
  def main(args: Array[String]): Unit = {
    val df = DataProcessing.dataProcessing()
    val dfWithSentiments = SentimentAnalysis.getSentiments(df)
    KafkaConsumer.writeStream(dfWithSentiments)
  }
}