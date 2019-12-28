from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("demo02") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=/home/spark/spark-2.4.4-bin-hadoop2.7/metastore_db") \
    .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver") \
    .enableHiveSupport() \
    .getOrCreate()

tables = spark.catalog.listTables()
for table in tables:
    print(table)

books = spark.read.table("hbooks")
books.show(truncate=False)

spark.stop()
