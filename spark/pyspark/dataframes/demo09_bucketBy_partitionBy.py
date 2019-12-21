from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("demo09") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

emps = spark.read \
    .parquet("/tmp/data01_csv")

emps.write.saveAsTable("emp1")
print("table saved.")

emps.write.partitionBy("deptno").saveAsTable("emp2")
print("partitioned table saved.")

emps.write.bucketBy(2, "empno").saveAsTable("emp3")
print("bucketed table saved.")

emps.write.partitionBy("deptno").bucketBy(2, "empno").saveAsTable("emp4")
print("partitioned bucketed table saved.")

spark.stop()
