import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Implements Streaming data processor using Spark SQL Stream
 */
object MainApp {

  def main(args: Array[String]): Unit = {
    val kafkaBroker = "kafka-broker:9092"
    val definitionTopic = "definitions"
    val wordCountTopic = "word-count-results"

    val spark: SparkSession = SparkSession.builder()
      .appName(s"SparkWordCountStreaming")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // defining input stream data type (word, definition, response_topic)
    val definitionSchema = new StructType()
      .add(StructField("word", StringType, nullable = true))
      .add(StructField("definition", StringType, nullable = true))
      .add(StructField("response_topic", StringType, nullable = true))

    // reading data from kafka topic
    val inputStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", definitionTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    inputStream.printSchema() // debug purpose

    // Udf function to use to transform our input data
    val transformationUdf = udf(transform _)

    // perform transformation here
    val outputDf = inputStream.selectExpr("cast(value as string)")
      .select(from_json(col("value"), definitionSchema).as("data"))
      .select(col("data.word"),
        transformationUdf(col("data.word"), col("data.definition")) // don't forget to apply the transformation
          .as("definition"))
      .select(col("word"), explode(col("definition")))
      .toDF("word", "token", "count")
      //.where(col("count") > 1) // filter out words with less than 2 occurrences
      //.filter(len(col("token")) > 2) // filter out words with less than 2 occurrences

    outputDf.printSchema() // debug purpose

    // displaying the transformed data to the console for debug purpose
    val streamConsoleOutput = outputDf.writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()

    // sending the transformed data to kafka
    outputDf
      .select(to_json(struct(col("word"),
        col("token"), col("count"))).as("value")) // compute a mandatory field `value` for kafka
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", wordCountTopic)
      .option("checkpointLocation", "/tmp/checkpoint") // required in kafka mode (the behaviour hard coded in the api!)
      .start()

    // waiting the query to complete (blocking call)
    streamConsoleOutput.awaitTermination()
  }

  private def transform(word: String, definition: String): Map[String, Int] = {
    val result = definition.toLowerCase.split("\\W+") // split by non-word characters
      .filter(_.nonEmpty) // remove empty strings
      .groupBy(identity) // group by word
      .mapValues(_.length) // count words

    // e.g. remove stop words, apply stemming, etc.
    // remove stop words
    val stopWords = Set(word, "so", "a", "an", "the", "is", "are", "am", "and", "or", "not", "for", "to", "in", "on", "at", "by", "with", "as", "of", "from", "that", "this", "these", "those", "there", "here", "where", "when", "how", "why", "what", "which", "who", "whom", "whose", "whom", "whomsoever", "whosoever", "whosever", "whosesoever")
    val cleanedWords = result.filterKeys(!stopWords.contains(_))

    // apply stemming

    // you can apply other transformation here as per your inspiration

    cleanedWords
  }
}