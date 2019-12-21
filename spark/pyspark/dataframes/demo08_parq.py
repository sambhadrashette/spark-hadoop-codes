from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("demo05") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

emps = spark.read \
    .parquet("/tmp/data01_csv")

emps.write \
    .mode("overwrite") \
    .saveAsTable("emps01")
print("Table saved")

spark.stop()
