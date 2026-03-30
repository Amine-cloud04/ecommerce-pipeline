package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Ecommerce Pipeline")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // ── INGEST ──────────────────────────────────────────────
    val customers = spark.read.option("header","true").option("inferSchema","true").csv("data/customers.csv")
    val products  = spark.read.option("header","true").option("inferSchema","true").csv("data/products.csv")
    val orders    = spark.read.option("header","true").option("inferSchema","true").csv("data/orders.csv")

    // ── CLEAN ───────────────────────────────────────────────

    // 1. Drop rows where critical fields are null
    val cleanOrders = orders
      .dropDuplicates()
      .filter(col("customer_id").isNotNull)
      .filter(col("product_id").isNotNull)
      .withColumn("quantity", 
        when(col("quantity").isNull, lit(1)).otherwise(col("quantity").cast("int"))
      )
      .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
      .withColumn("customer_id", col("customer_id").cast("int"))
      .withColumn("product_id",  col("product_id").cast("int"))

    val cleanCustomers = customers
      .dropDuplicates()
      .withColumnRenamed("name", "customer_name")

    val cleanProducts  = products.dropDuplicates()

    println("\n========== CLEANING REPORT ==========")
    println(s"Orders  : ${orders.count()} raw  →  ${cleanOrders.count()} clean")
    println(s"Customers: ${customers.count()} raw  →  ${cleanCustomers.count()} clean")
    println(s"Products : ${products.count()} raw  →  ${cleanProducts.count()} clean")

    println("\nClean orders sample:")
    cleanOrders.show(5)

    // ── TRANSFORM ───────────────────────────────────────────

    // 2. Join everything into one big flat table
    val enriched = cleanOrders
      .join(cleanCustomers, "customer_id")
      .join(cleanProducts,  "product_id")
      .withColumn("revenue", col("quantity") * col("price"))
      .withColumn("month", month(col("order_date")))
      .withColumn("year",  year(col("order_date")))
      .filter(col("status") === "completed")

    println("\n========== ENRICHED TABLE ==========")
    enriched.printSchema()
    enriched.show(5)

    // ── KPIs ────────────────────────────────────────────────

    // 3. Total revenue
    val totalRevenue = enriched.agg(round(sum("revenue"), 2).as("total_revenue"))

    // 4. Revenue by category
    val revenueByCategory = enriched
      .groupBy("category")
      .agg(round(sum("revenue"), 2).as("revenue"))
      .orderBy(desc("revenue"))

    // 5. Top 5 products
    val topProducts = enriched
      .groupBy("name")
      .agg(
        sum("quantity").as("units_sold"),
        round(sum("revenue"), 2).as("revenue")
      )
      .orderBy(desc("revenue"))
      .limit(5)

    // 6. Top 10 most active customers
    val topCustomers = enriched
      .groupBy(
      col("customer_id"),
      col("name"),
      col("city"),
      col("country")
  )
      .agg(
      count("order_id").as("total_orders"),
      round(sum("revenue"), 2).as("total_spent")
  )
      .withColumnRenamed("name", "customer_name")
      .orderBy(desc("total_spent"))
      .limit(10)

    // 7. Monthly revenue trend
    val monthlyRevenue = enriched
      .groupBy("year", "month")
      .agg(round(sum("revenue"), 2).as("revenue"))
      .orderBy("year", "month")

    // 8. Sales by country
    val salesByCountry = enriched
      .groupBy("country")
      .agg(
        count("order_id").as("orders"),
        round(sum("revenue"), 2).as("revenue")
      )
      .orderBy(desc("revenue"))

    println("\n========== KPI RESULTS ==========")

    print("\n--- Total Revenue ---\n")
    totalRevenue.show()

    print("\n--- Revenue by Category ---\n")
    revenueByCategory.show()

    print("\n--- Top 5 Products ---\n")
    topProducts.show()

    print("\n--- Top 10 Customers ---\n")
    topCustomers.show()

    print("\n--- Monthly Revenue ---\n")
    monthlyRevenue.show(12)

    print("\n--- Sales by Country ---\n")
    salesByCountry.show()

    // ── STORE ───────────────────────────────────────────────
    enriched.write.mode("overwrite").parquet("output/enriched_orders")
    revenueByCategory.write.mode("overwrite").parquet("output/revenue_by_category")
    topProducts.write.mode("overwrite").parquet("output/top_products")
    topCustomers.write.mode("overwrite").parquet("output/top_customers")
    monthlyRevenue.write.mode("overwrite").parquet("output/monthly_revenue")
    salesByCountry.write.mode("overwrite").parquet("output/sales_by_country")

    println("\n========== STORAGE ==========")
    println("All results saved to output/ as Parquet files:")
    println("  output/enriched_orders")
    println("  output/revenue_by_category")
    println("  output/top_products")
    println("  output/top_customers")
    println("  output/monthly_revenue")
    println("  output/sales_by_country")

    spark.stop()
    println("\n[Day 2 complete] Pipeline ran successfully!")
  }
}
