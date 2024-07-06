from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs
from pyspark.sql.functions import window, col, sum

spark = SparkSession.builder.master("local[2]") \
    .appName("SensorDataStreaming").getOrCreate()

socket_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

split_df = socket_df.selectExpr("split(value, ',') AS data")

parsed_df = split_df.selectExpr(
    "data[0] as plant_id",
    "data[1] as timestamp",
    "data[3] as plocha",
    "data[4] as dc_power",
    "data[5] as ac_power"
)
timestamped_wide_df = (parsed_df
                       .withColumn("timestamp1", funcs.to_timestamp("timestamp", "dd-MM-yyyy HH:mm"))
                       .withColumn("timestamp2", funcs.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")))
timestamped_df = (timestamped_wide_df.withColumn("timestamp", funcs.when(funcs.col("timestamp1").isNotNull(),
                                                                         funcs.col("timestamp1")).otherwise(
    funcs.col("timestamp2"))).drop("timestamp1", "timestamp2"))

grouped_df = timestamped_df.withWatermark("timestamp", "10 minutes") \
    .groupBy(
    window(col("timestamp"), "60 minutes", "15 minutes"),
    col("plant_id")) \
    .agg(sum("dc_power").alias("total_dc_power"), sum("ac_power").alias("total_ac_power")) \
    .select(
    col("window.start").alias("timestamp_from"),
    col("window.end").alias("timestamp_to"),
    col("plant_id"),
    col("total_ac_power"),
    col("total_dc_power")
)

q1 = grouped_df.writeStream \
    .format("csv") \
    .trigger(processingTime="4 seconds") \
    .option("checkpointLocation", "checkpoint/") \
    .option("path", "out/") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
