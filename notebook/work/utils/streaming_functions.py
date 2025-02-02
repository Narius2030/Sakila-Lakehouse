import sys
sys.path.append("./work/streamify")

from pyspark.sql.functions import *
from utils.schema import schema_taxi

def process_stream(stream):
	stream = stream \
			.selectExpr("CAST(value AS STRING)") \
			.select(from_json(col("value"), schema_taxi).alias("data"))

	stream = stream \
		.groupBy("data.pickup_location")\
		.agg(avg("data.trip_distance").alias("avg_trip_distance"), avg("data.total_amount").alias("avg_total_amount"))

	return stream
                