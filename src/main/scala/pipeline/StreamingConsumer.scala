package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object StreamingConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EnrichedEcommerceStream")
      .master("local[*]")
      .config("spark.es.nodes", "localhost")
      .config("spark.es.port", "9200")
      .config("spark.es.nodes.wan.only", "true")
      .getOrCreate()

    // Reduce logging noise to see our custom messages clearly
    spark.sparkContext.setLogLevel("WARN")

    // 1. Load Products Reference Data (Cast ID to String for reliable joining)
    val productsDF = spark.read
      .option("header", "true")
      .csv("data/products.csv")
      .select(
        col("product_id").cast("string"), 
        col("name").as("product_name"), 
        col("category"), 
        col("price").cast("double")
      )

    // 2. Load Customers Reference Data (Split name into first_name)
    val customersDF = spark.read
      .option("header", "true")
      .csv("data/customers.csv")
      .withColumn("first_name", split(col("name"), " ").getItem(0))
      .select(
        col("customer_id").cast("string"), 
        col("first_name"), 
        col("city")
      )

    // 3. Define the Schema for incoming Kafka JSON
    val orderSchema = new StructType()
      .add("order_id", StringType)
      .add("customer_id", StringType)
      .add("product_id", StringType)
      .add("quantity", DoubleType)
      .add("order_date", StringType)
      .add("status", StringType)

    // 4. Connect to Kafka
    val kafkaStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "user_orders")
      .option("startingOffsets", "earliest")
      .load()

    // 5. Parse JSON and Apply Defensive Filtering
    val ordersStream = kafkaStream.selectExpr("CAST(value AS STRING) as json_str")
      .withColumn("data", from_json(col("json_str"), orderSchema))
      .select("data.*")
      .filter(
        col("order_id").isNotNull &&
        col("quantity").isNotNull && !col("quantity").isNaN &&
        col("customer_id").isNotNull
      )

    // 6. Execute the Joins (Enrichment)
    val enrichedStream = ordersStream
      .join(productsDF, Seq("product_id"), "left")
      .join(customersDF, Seq("customer_id"), "left")
      .withColumn("total_amount", round(col("price") * col("quantity"), 2))

    println("🚀 ENRICHED & FILTERED STREAM STARTED. Processing live orders...")

    // 7. Write to Elasticsearch with a clean versioned index
    val query = enrichedStream.writeStream
      .format("org.elasticsearch.spark.sql")
      .outputMode("append")
      .option("checkpointLocation", "/tmp/spark-checkpoints-final-v1")
      .option("es.resource", "enriched_orders_final")
      .option("es.mapping.id", "order_id")
      .start()

    query.awaitTermination()
  }
}
