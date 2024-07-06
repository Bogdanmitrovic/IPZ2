from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType, IntegerType

spark = SparkSession.builder.master("local[2]") \
    .appName("Coalesce").getOrCreate()

csv_schema = StructType([
    StructField("timestamp-from", TimestampType(), True),
    StructField("timestamp-to", TimestampType(), True),
    StructField("plant_id", IntegerType(), True),
    StructField("total_dc_power", FloatType(), True),
    StructField("total_ac_power", FloatType(), True)
])

allfiles = spark.read.csv("out/*.csv", header=False, schema=csv_schema)
allfiles.coalesce(1).write.option("header", "true").csv("final")
