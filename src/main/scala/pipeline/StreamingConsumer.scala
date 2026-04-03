package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamingConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CryptoLive")
      .master("local[1]")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.es.nodes", "localhost")
      .config("spark.es.port", "9200")
      .config("spark.es.nodes.wan.only", "true")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val schema = StructType(Array(
      StructField("order_id", StringType),
      StructField("customer_id", StringType),
      StructField("product_id", StringType),
      StructField("quantity", DoubleType),
      StructField("order_date", StringType),
      StructField("status", StringType)
    ))

    val productsDF = spark.read.option("header", "true").csv("data/products.csv")

    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user_orders")
      .load()

    val apiStream = kafkaStream.selectExpr("CAST(value AS STRING) as json_str")
      .withColumn("data", from_json(col("json_str"), schema))
      .select("data.*")

    val enrichedStream = apiStream.join(productsDF, Seq("product_id"), "left")
      .withColumn("live_price", col("quantity")) // Renaming for clarity in Kibana

    val query = enrichedStream.writeStream
      .format("org.elasticsearch.spark.sql")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/spark-checkpoints-crypto")
      .option("es.resource", "crypto_live_final")
      .start()

    query.awaitTermination()
  }
}
