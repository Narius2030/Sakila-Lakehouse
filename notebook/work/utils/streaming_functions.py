import sys
sys.path.append("./work/streamify")
import pyspark.sql.functions as F

def rental_process_stream(stream, stream_schema):
    stream = (stream 
				.selectExpr("CAST(value AS STRING)")
				.select(F.from_json(F.col("value"), "STRUCT<after STRING>").alias("json_data"))
                .select(F.from_json(F.col("json_data.after"), stream_schema).alias("data"))
                .select(F.col("data.*")))
    
    stream = (stream 
                .withColumn("rental_date", F.from_unixtime(F.col("rental_date") / 1000000, "yyyy-MM-dd HH:mm:ss"))
                .withColumn("return_date", F.from_unixtime(F.col("return_date") / 1000000, "yyyy-MM-dd HH:mm:ss"))
                .withColumn("last_update", F.from_unixtime(F.col("last_update") / 1000000, "yyyy-MM-dd HH:mm:ss")))

    return stream


def payment_process_stream(stream, stream_schema):
    stream = (stream 
				.selectExpr("CAST(value AS STRING)")
				.select(F.from_json(F.col("value"), "STRUCT<after STRING>").alias("json_data"))
                .select(F.from_json(F.col("json_data.after"), stream_schema).alias("data"))
                .select(F.col("data.*")))
    
    stream = (stream 
                .withColumn("payment_date", F.from_unixtime(F.col("payment_date") / 1000000, "yyyy-MM-dd HH:mm:ss"))
                .withColumn("last_update", F.from_unixtime(F.col("last_update") / 1000000, "yyyy-MM-dd HH:mm:ss")))

    return stream