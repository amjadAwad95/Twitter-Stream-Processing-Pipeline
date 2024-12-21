object Main {
  def main(args: Array[String]): Unit = {
    val df = DataProcessing.dataProcessing()
    KafkaConsumer.writeStream(df)
  }
}