import org.apache.spark.sql.DataFrame
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.sentiment._
import edu.stanford.nlp.ling._
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import org.apache.spark.sql.functions.udf
import java.util.Properties
import scala.collection.JavaConverters._

object SentimentAnalysis {

  def getSentiments(df: DataFrame): DataFrame = {

    // CoreNLP pipeline properties
    val props = new Properties()
    props.setProperty("annotators", "tokenize,ssplit,parse,sentiment")

    // udf to compute sentiments for each text
    val computeSentiments = udf((text: String) => {

      val pipeline = new StanfordCoreNLP(props)
      val annotation = new Annotation(text)
      pipeline.annotate(annotation)

      // extracting sentiment from sentences
      val sentenceList = annotation
        .get(classOf[CoreAnnotations.SentencesAnnotation])
        .asScala
        .toList

      // collecting sentiments into a set for each tweet
      sentenceList.map { sentence =>
        val tree = sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])
        val score = RNNCoreAnnotations.getPredictedClass(tree)
        score match {
          case 0 => "Very Negative"
          case 1 => "Negative"
          case 2 => "Neutral"
          case 3 => "Positive"
          case 4 => "Very Positive"
        }
      }.toSet
    })

    df.withColumn("sentiments", computeSentiments(df("text")))
  }
}
