from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from transforms.utils import LOCAL_SPARK, get_current_calmonth, get_lakehouse_dir

spark = (
    SparkSession.builder.remote(LOCAL_SPARK).getOrCreate() # type: ignore
)

_, year, month, day = get_current_calmonth()
lk_main_path = get_lakehouse_dir()

bronze_path = f"{lk_main_path}/bronze/breweries/*/{year}/{month}/{day}/breweries.json"
df = spark.read.json(bronze_path)

df = df.select(
    col("id").alias("brewery_id"),
    col("address_1"),
    col("address_2"),
    col("address_3"),
    col("brewery_type"),
    col("city"),
    col("country"),
    col("latitude"),
    col("longitude"),
    col("name").alias("brewery_name"),
    col("phone"),
    col("postal_code"),
    col("state"),
    col("state_province"),
    col("street"),
    col("website_url")
).withColumn("ldts", current_timestamp())

spark.sql("""
    CREATE TABLE IF NOT EXISTS lakehouse.silver.breweries (
        brewery_id STRING,
        brewery_name STRING,
        brewery_type STRING,
        address_1 STRING,
        address_2 STRING,
        address_3 STRING,
        city STRING,
        state_province STRING,
        postal_code STRING,
        country STRING,
        longitude DOUBLE,
        latitude DOUBLE,
        phone STRING,
        website_url STRING,
        ldts TIMESTAMP
    ) USING iceberg
    PARTITIONED BY (country)
""")

# If needed to recreate due to schema change
# spark.sql("""
#     CREATE OR REPLACE TABLE lakehouse.silver.breweries (
#         brewery_id STRING,
#         brewery_name STRING,
#         brewery_type STRING,
#         address_1 STRING,
#         address_2 STRING,
#         address_3 STRING,
#         city STRING,
#         state_province STRING,
#         postal_code STRING,
#         country STRING,
#         longitude DOUBLE,
#         latitude DOUBLE,
#         phone STRING,
#         website_url STRING,
#         ldts TIMESTAMP
#     ) USING iceberg
#     PARTITIONED BY (country)
# """)

df.createOrReplaceTempView("stg_glb_breweries")

spark.sql("""
    MERGE INTO lakehouse.silver.breweries AS target
    USING stg_glb_breweries AS source
    ON target.brewery_id = source.brewery_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")