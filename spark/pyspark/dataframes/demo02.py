from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Create spark context
spark = SparkSession.builder \
    .appName("demo02") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Define schema data types
emp_schema = StructType([
    StructField("empno", IntegerType(), False),
    StructField("ename", StringType(), True),
    StructField("job", StringType(), True),
    StructField("mgr", IntegerType(), True),
    StructField("hire", DateType(), True),
    StructField("sal", DoubleType(), True),
    StructField("comm", DoubleType(), True),
    StructField("deptno", IntegerType(), True)
])

emps1 = spark.read \
    .option("nullValue", "NULL") \
    .schema(emp_schema) \
    .csv("data/emp.csv")

emps1.printSchema()
emps1.show()

emps2 = spark.read \
    .option("nullValue", "NULL") \
    .schema("empno INT, ename STRING, job STRING, mgr INT, hire DATE, sal DOUBLE, comm DOUBLE, deptno INT") \
    .csv("data/emp.csv")

emps2.printSchema()
emps2.show()

spark.stop()
