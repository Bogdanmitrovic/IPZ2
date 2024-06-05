from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from pyspark.sql.functions import window, avg, col

spark = SparkSession.builder.master("local[2]") \
    .appName("SensorDataStreaming").getOrCreate()

socketDF = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

split_df = socketDF.selectExpr("split(value, ',') AS data")

parsed_df = split_df.selectExpr(
    "data[0] as timestamp",
    "data[2] as plocha",
    "CASE WHEN data[3] = 0 THEN NULL ELSE CAST(data[3] AS INTEGER) END as dc_power",
    "CASE WHEN data[4] = 0 THEN NULL ELSE CAST(data[4] AS INTEGER) END as ac_power"
)

timestamped_df = parsed_df.withColumn("timestamp", funcs.to_timestamp("timestamp", "dd-MM-yyyy HH:mm"))

windowed_df = timestamped_df.withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "60 minutes", "15 minutes")) \
    .agg(avg("dc_power").alias("avg_dc_power"), avg("ac_power").alias("avg_ac_power"))

q1 = windowed_df.select('window.start', 'window.end', 'avg_dc_power', 'avg_ac_power').writeStream \
    .format("csv") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "checkpoint/") \
    .option("path", "out/") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

q2 = windowed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()
q2.awaitTermination()
