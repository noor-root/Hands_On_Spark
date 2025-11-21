# lab1_hello_spark.py
import os, sys
# forcer Spark à utiliser le même python (évite "Python worker failed to connect back")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def main():
    spark = SparkSession.builder \
        .appName("lab1-hello-spark") \
        .master("local[*]") \
        .config("spark.python.worker.faulthandler.enabled","true") \
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled","true") \
        .getOrCreate()

    # 1) Display environment info
    print("Python executable:", sys.executable)
    print("Spark version:", spark.version)
    print("Spark master:", spark.sparkContext.master)
    print("Default parallelism:", spark.sparkContext.defaultParallelism)

    # 2) Static DataFrame
    data = [
        (1, "Alice", 34, "FR"),
        (2, "Bob", 45, "UK"),
        (3, "Cathy", 29, "FR"),
        (4, "David", 40, "DE"),
        (5, "Eve", 34, "UK"),
    ]
    columns = ["id", "name", "age", "country"]
    df = spark.createDataFrame(data, schema=columns)

    # show schema & preview
    print("\nSchema:")
    df.printSchema()

    print("\nPreview (df.show):")
    df.show()

    # Aggregation
    print("\nAggregations: average age")
    df.agg(avg(col("age")).alias("avg_age")).show()

    # Filter
    print("\nFilter: age > 35")
    df.filter(col("age") > 35).show()

    # Count (action)
    n = df.count()
    print(f"\nTotal rows: {n}")

    # pause to keep UI 4040 visible while you take screenshots
    input("\nApplication running — app UI available on http://localhost:4040 . Appuyez sur Entrée pour terminer et arrêter Spark...\n")

    spark.stop()
    print("Spark stopped.")

if __name__ == "__main__":
    main()
