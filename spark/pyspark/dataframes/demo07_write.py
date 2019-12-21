from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("demo05") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

emps = spark.read \
    .option("user", "root") \
    .option("password", "manager") \
    .jdbc("jdbc:mysql://localhost:3306/sp03", "emp")

emps.write \
    .mode("overwrite") \
    .json("/tmp/data01_json")
print("json written")

emps.write \
    .mode("overwrite") \
    .csv("/tmp/data01_csv")
print("csv written")

emps.write \
    .mode("overwrite") \
    .save("/tmp/data01_csv")
print("saved parquet file")

spark.stop()
