import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{SparkSession, functions}

object TestStreaming {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("streaming-test")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val BROKERS = "localhost:9092"
    val INPUT_TOPIC = "demo-spark"
    val checkpointDir = "chkdir"

    val inputDf = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", INPUT_TOPIC)
      .option("startingOffsets", "latest")
      .load

    val wordsDF = inputDf.selectExpr("CAST(value AS STRING)")
    //.agg(functions.max("value"))

    wordsDF
      .writeStream
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 second"))
      //.option("checkpointLocation", checkpointDir)
      .format("console")
      .start()
      .awaitTermination()

    spark.close()
  }

  def exemple(spark: SparkSession): Unit = {
    import spark.implicits._

    val BROKERS = "localhost:9092"
    val INPUT_TOPIC_CSV = "test-topic"

    val inputDf = spark
      .readStream
      .format("rate") // <-- use RateStreamSource
      .option("rowsPerSecond", 3)
      .load

    println("CSV")
    //    val csvDF = inputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    val csvDF = inputDf.selectExpr("CAST(value AS LONG)")
      .withColumn("new", $"value" > 15)
      .agg(functions.sum("value"))

    csvDF.writeStream
      .outputMode("complete")
      .format("console")
      .start()
      .awaitTermination()
  }

}


