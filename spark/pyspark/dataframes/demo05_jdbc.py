from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("demo05") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

emps = spark.read \
    .option("user", "root") \
    .option("password", "manager") \
    .jdbc("jdbc:mysql://localhost:3306/sp03", "emp")

emps.show(truncate=False)

spark.stop()
