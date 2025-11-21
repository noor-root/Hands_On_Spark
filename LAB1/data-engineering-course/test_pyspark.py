from pyspark.sql import SparkSession
# Avoid warnings
import warnings

warnings.filterwarnings('ignore')

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.showConsoleProgress=false pyspark-shell'
print("=" * 60)
print("Part B - PySpark Installation Validation")
print("=" * 60)

# Initialize SparkSession
print("\n[1] Starting SparkSession...")
spark = SparkSession.builder \
    .appName("PySpark_Installation_Test") \
    .master("local[*]") \
    .getOrCreate()

print("SparkSession started successfully!")

# Display Spark version
print("\n[2] Spark Version Information:")
print("-" * 60)
print(f"Spark Version: {spark.version}")
print(f"Application Name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")
print("-" * 60)

# Stop the session properly
print("\n[3] Stopping SparkSession...")
spark.stop()
print("Session stopped properly.")
print("=" * 60)
print("Part B completed successfully!")
print("=" * 60)