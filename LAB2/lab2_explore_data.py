from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# -------- 2.1 SparkSession --------

spark = (
    SparkSession.builder
    .appName("Day1-DataExploration")
    .master("local[*]")  # Mode local - pas besoin du cluster Docker
    .config("spark.driver.memory", "2g")
    .config("spark.driver.host", "localhost")
    .getOrCreate()
)

# Réduire les logs pour plus de clarté
spark.sparkContext.setLogLevel("WARN")

print("SparkSession created:", spark.sparkContext.appName)

# -------- 2.2 Load CSVs --------
customers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("spark-data/ecommerce/customers.csv")
).cache()

products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("spark-data/ecommerce/products.csv")
).cache()

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("spark-data/ecommerce/orders.csv")
).cache()

print("Datasets loaded successfully.")

# -------- 2.3 Inspect schemas --------
print("=== CUSTOMERS Schema ===")
customers.printSchema()
print("=== PRODUCTS Schema ===")
products.printSchema()
print("=== ORDERS Schema ===")
orders.printSchema()

# -------- 2.4 Basic statistics (size) --------
total_customers = customers.count()
total_products = products.count()
total_orders = orders.count()

print(f"Total customers: {total_customers}")
print(f"Total products: {total_products}")
print(f"Total orders: {total_orders}")

# -------- 2.5 Data preview --------
print("Customers preview:")
customers.show(5, truncate=False)
print("Products preview:")
products.show(5, truncate=False)
print("Orders preview:")
orders.show(5, truncate=False)

# -------- 2.6 Data quality checks (nulls & duplicates) --------
def null_counts(df):
    exprs = [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]
    return df.select(*exprs)

print("Null counts in customers:")
null_counts(customers).show(truncate=False)
print("Null counts in orders:")
null_counts(orders).show(truncate=False)

# duplicate ID checks
cust_count = customers.count()
cust_distinct = customers.select("customerNumber").distinct().count()
cust_duplicates = cust_count - cust_distinct
print(f"Customer duplicates: {cust_duplicates} (count {cust_count} vs distinct {cust_distinct})")

orders_count = orders.count()
orders_distinct = orders.select("orderNumber").distinct().count()
orders_duplicates = orders_count - orders_distinct
print(f"Order duplicates: {orders_duplicates} (count {orders_count} vs distinct {orders_distinct})")

# -------- 2.7 Exploratory analysis --------
print("Customers by segment:")
customers.groupBy("customerSegment").count().orderBy(F.desc("count")).show()

print("Top 10 countries by customer count:")
customers.groupBy("country").count().orderBy(F.desc("count")).show(10)

print("Orders by status:")
orders.groupBy("status").count().orderBy(F.desc("count")).show()

print("Orders by paymentMethod:")
orders.groupBy("paymentMethod").count().orderBy(F.desc("count")).show()

print("Products by category:")
products.groupBy("productCategory").count().orderBy(F.desc("count")).show()

# -------- 2.8 Numerical analysis --------
print("Order amount statistics:")
orders.select(
    F.count("totalAmount").alias("count"),
    F.min("totalAmount").alias("min"),
    F.max("totalAmount").alias("max"),
    F.avg("totalAmount").alias("avg"),
    F.sum("totalAmount").alias("sum")
).show()

print("Credit limit statistics by segment:")
customers.groupBy("customerSegment").agg(
    F.count("creditLimit").alias("count"),
    F.avg("creditLimit").alias("avg_creditLimit"),
    F.max("creditLimit").alias("max_creditLimit")
).orderBy(F.desc("avg_creditLimit")).show()

print("Product price statistics (buyPrice, MSRP):")
products.select(
    F.min("buyPrice").alias("min_buy"),
    F.max("buyPrice").alias("max_buy"),
    F.avg("buyPrice").alias("avg_buy"),
    F.min("MSRP").alias("min_msrp"),
    F.max("MSRP").alias("max_msrp"),
    F.avg("MSRP").alias("avg_msrp")
).show()

# -------- 2.9 Summary report export --------
import os

# Calcul des métriques
total_revenue = orders.agg(F.sum("totalAmount")).collect()[0][0] or 0.0
avg_order_value = orders.agg(F.avg("totalAmount")).collect()[0][0] or 0.0

# Créer le dossier summary s'il n'existe pas
out_dir = "spark-data/ecommerce/summary"
os.makedirs(out_dir, exist_ok=True)

# Écrire le CSV avec Python standard (contourne le problème winutils sur Windows)
out_path = os.path.join(out_dir, "summary.csv")
with open(out_path, "w") as f:
    f.write("Metric,Value\n")
    f.write(f"Total Customers,{total_customers}\n")
    f.write(f"Total Products,{total_products}\n")
    f.write(f"Total Orders,{total_orders}\n")
    f.write(f"Total Revenue,{round(total_revenue, 2)}\n")
    f.write(f"Average Order Value,{round(avg_order_value, 2)}\n")

print(f"Summary written to {out_path}")

# Afficher le résumé
print("\nSummary Report:")
print(f"  Total Customers: {total_customers}")
print(f"  Total Products: {total_products}")
print(f"  Total Orders: {total_orders}")
print(f"  Total Revenue: {round(total_revenue, 2)}")
print(f"  Average Order Value: {round(avg_order_value, 2)}")


# ============================================================
# SECTION 9: QUESTIONS BUSINESS (Part C)
# ============================================================

print("\n" + "="*60)
print("SECTION 9: QUESTIONS BUSINESS")
print("="*60)

# ----- Question 1: Pays avec le plus haut total de crédit -----
print("\n--- Q1: Quel pays a le plus haut total de creditLimit? ---")
q1_result = (
    customers
    .groupBy("country")
    .agg(F.sum("creditLimit").alias("total_credit"))
    .orderBy(F.desc("total_credit"))
    .limit(1)
)
q1_result.show(truncate=False)

# ----- Question 2: Statut de commande le plus fréquent -----
print("\n--- Q2: Quel est le statut de commande le plus fréquent? ---")
q2_result = (
    orders
    .groupBy("status")
    .count()
    .orderBy(F.desc("count"))
    .limit(1)
)
q2_result.show(truncate=False)

# ----- Question 3: Catégorie produit avec le plus de stock -----
print("\n--- Q3: Quelle catégorie a le plus grand total de quantityInStock? ---")
q3_result = (
    products
    .groupBy("productCategory")
    .agg(F.sum("quantityInStock").alias("total_stock"))
    .orderBy(F.desc("total_stock"))
    .limit(1)
)
q3_result.show(truncate=False)

# ----- Question 4: Pourcentage de clients Enterprise -----
print("\n--- Q4: Quel pourcentage de clients sont 'Enterprise'? ---")
total_cust = customers.count()
enterprise_cust = customers.filter(F.col("customerSegment") == "Enterprise").count()
percentage = (enterprise_cust / total_cust) * 100
print(f"Clients Enterprise: {enterprise_cust}")
print(f"Total clients: {total_cust}")
print(f"Pourcentage Enterprise: {round(percentage, 2)}%")

# ----- Question 5: Distribution des commandes par mois -----
print("\n--- Q5: Combien de commandes par mois? ---")
q5_result = (
    orders
    .withColumn("order_month", F.month(F.to_date(F.col("orderDate"), "yyyy-MM-dd")))
    .groupBy("order_month")
    .count()
    .orderBy("order_month")
)
q5_result.show(12)


# ============================================================
# FIN DU SCRIPT
# ============================================================

print("\n" + "="*60)
print("END OF SCRIPT - All tasks completed successfully!")
print("="*60)

spark.stop()
print("SparkSession stopped.")