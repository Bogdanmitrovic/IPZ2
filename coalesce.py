from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.master("local[2]") \
    .appName("Coalesce").getOrCreate()

csv_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("plocha", StringType(), True),
    StructField("dc_power", StringType(), True),
    StructField("ac_power", StringType(), True)
])

allfiles = spark.read.csv("out/*.csv", header=False, schema=csv_schema)
allfilesfiltered = allfiles.filter(allfiles.ac_power.isNotNull())
allfilesfiltered.coalesce(1).write.option("header", "true").csv("final")
