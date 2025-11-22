from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, desc, min, max, avg, sum

# -------- 2.1 Create SparkSession --------
spark = (
    SparkSession.builder
    .appName("Day1-DataExploration")
    .master("spark://localhost:7077")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
print("SparkSession created: Day1-DataExploration")

# -------- 2.2 Load CSV datasets --------
customers = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("spark-data/ecommerce/customers.csv")
    .cache()
)
products = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("spark-data/ecommerce/products.csv")
    .cache()
)
orders = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("spark-data/ecommerce/orders.csv")
    .cache()
)
print("Datasets loaded successfully.")

# -------- 2.3 Inspect schemas --------
print("=== CUSTOMERS Schema ===")
customers.printSchema()
print("=== PRODUCTS Schema ===")
products.printSchema()
print("=== ORDERS Schema ===")
orders.printSchema()

# -------- 2.4 Basic statistics (size) --------
print(f"Total customers: {customers.count()}")
print(f"Total products: {products.count()}")
print(f"Total orders: {orders.count()}")

# -------- 2.5 Data preview --------
print("Customers preview:")
customers.show(5, truncate=False)
print("Products preview:")
products.show(5, truncate=False)
print("Orders preview:")
orders.show(5, truncate=False)

# -------- 2.6 Data quality checks --------
print("Null counts in customers:")
customers.select([count(when(col(c).isNull(), c)).alias(c) for c in customers.columns]).show()

print("Null counts in orders:")
orders.select([count(when(col(c).isNull(), c)).alias(c) for c in orders.columns]).show()

# Check duplicates
customer_dupes = customers.count() - customers.select("customerNumber").distinct().count()
order_dupes = orders.count() - orders.select("orderNumber").distinct().count()
print(f"Customer duplicates: {customer_dupes}")
print(f"Order duplicates: {order_dupes}")

# -------- 2.7 Exploratory analysis --------
print("Customers by segment:")
customers.groupBy("customerSegment").count().orderBy(desc("count")).show()

print("Top 10 countries by customer count:")
customers.groupBy("country").count().orderBy(desc("count")).show(10)

print("Orders by status:")
orders.groupBy("status").count().show()

print("Orders by paymentMethod:")
orders.groupBy("paymentMethod").count().show()

print("Products by category:")
products.groupBy("productCategory").count().show()

# -------- 2.8 Numerical analysis --------
print("Order amount statistics:")
orders.select(
    count("totalAmount").alias("count"),
    min("totalAmount").alias("min"),
    max("totalAmount").alias("max"),
    avg("totalAmount").alias("avg"),
    sum("totalAmount").alias("sum")
).show()

print("Credit limit statistics by segment:")
customers.groupBy("customerSegment").agg(
    count("creditLimit").alias("count"),
    avg("creditLimit").alias("avg_creditLimit"),
    max("creditLimit").alias("max_creditLimit")
).show()

print("Product price statistics:")
products.select(
    min("buyPrice").alias("min_buy"),
    max("buyPrice").alias("max_buy"),
    avg("buyPrice").alias("avg_buy"),
    min("MSRP").alias("min_msrp"),
    max("MSRP").alias("max_msrp"),
    avg("MSRP").alias("avg_msrp")
).show()

# -------- 2.9 Summary report export --------
total_customers = customers.count()
total_products = products.count()
total_orders = orders.count()
total_revenue = orders.select(sum("totalAmount")).collect()[0][0]
avg_order_value = orders.select(avg("totalAmount")).collect()[0][0]

summary_data = [
    ("Total Customers", total_customers),
    ("Total Products", total_products),
    ("Total Orders", total_orders),
    ("Total Revenue", total_revenue),
    ("Average Order Value", avg_order_value)
]

summary_df = spark.createDataFrame(summary_data, ["Metric", "Value"])
summary_df.coalesce(1).write.option("header", "true").mode("overwrite").csv("spark-data/ecommerce/summary/")

print("Summary report written to spark-data/ecommerce/summary/")
print("Lab2 exploration complete.")
spark.stop()
