from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .getOrCreate()
)


data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
columns = ["name", "id"]
df = spark.createDataFrame(data, columns)


(
 df
 .write.mode("overwrite")
 .parquet("ofs://om/lakehouse/staging/users.parquet")
)

