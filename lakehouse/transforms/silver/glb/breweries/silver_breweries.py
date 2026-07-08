from pyspark.sql import SparkSession
from transforms.utils import LOCAL_SPARK, get_current_calmonth

spark = (
    SparkSession.builder.remote(LOCAL_SPARK).getOrCreate() # type: ignore
)

_, year, month, day = get_current_calmonth()

countries = ["germany", "usa"]

for country in countries:
    breweries_key = f"ofs://om/lakehouse/bronze/breweries/{country}/{year}/{month}/{day}/breweries.json"
    df = (
        spark.read.json(breweries_key)
    )