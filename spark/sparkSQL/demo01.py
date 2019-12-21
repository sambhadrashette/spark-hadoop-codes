from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("demo01") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

emp_df = spark.read.parquet("/tmp/data01_csv")

emp_df.createOrReplaceTempView("emp_view")

result1 = spark.sql("SELECT job, AVG(sal) avgsal FROM emp_view GROUP BY job ORDER BY job")
result1.show()
result1.explain()

result2 = emp_df \
    .groupBy("job") \
    .avg("sal") \
    .orderBy("job")
result2.show()
result2.explain()

spark.stop()
