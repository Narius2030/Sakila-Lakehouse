import sys
sys.path.append("./work/streamify")

from utils.config import get_settings
from operators.streaming import SparkStreaming
from utils.streaming_functions import process_stream

settings = get_settings()
TOPIC="test"
STORAGE_PATH="s3a://lakehouse/streaming/test/taxi_trends"
CHECKPOINT_PATH = "s3a://lakehouse/streaming/test/checkpoint_taxi"

## TODO: create a SparkSession
spark = SparkStreaming.get_instance(app_name="Streamify Spark Streaming")

## TODO: create stream reader
stream = SparkStreaming.create_kafka_read_stream(spark, settings.KAFKA_ADDRESS, settings.KAFKA_PORT, TOPIC)

## TODO: process stream
processed_stream = process_stream(stream)

## TODO: write stream into Delta Lake
write_stream = SparkStreaming.create_file_write_stream(processed_stream, CHECKPOINT_PATH, STORAGE_PATH)
write_stream.start()

spark.streams.awaitAnyTermination()