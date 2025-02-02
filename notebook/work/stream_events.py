import sys
sys.path.append("./work/streamify")

from utils.config import get_settings
from operators.streaming import SparkStreaming
from utils.streaming_functions import process_stream
from utils.schema import schema_rental, schema_payment

settings = get_settings()
PAYMENT_TOPIC = "dbserver1.public.payment"
RENTAL_TOPIC = "dbserver1.public.rental"
STORAGE_PATH = "s3a://lakehouse/streaming/streamify/rental"
CHECKPOINT_PATH = "s3a://lakehouse/streaming/streamify/checkpoints/checkpoint_rental"

## TODO: create a SparkSession
spark = SparkStreaming.get_instance(app_name="Streamify Spark Streaming")

## TODO: create stream reader
rental_stream = SparkStreaming.create_kafka_read_stream(spark, settings.KAFKA_ADDRESS, settings.KAFKA_PORT, RENTAL_TOPIC)
# payment_stream = SparkStreaming.create_kafka_read_stream(spark, settings.KAFKA_ADDRESS, settings.KAFKA_PORT, PAYMENT_TOPIC)

## TODO: process stream
processed_rental = process_stream(rental_stream, schema_rental)

## TODO: write stream into Delta Lake
write_rental_stream = SparkStreaming.create_file_write_stream(processed_rental, CHECKPOINT_PATH, STORAGE_PATH, file_format="delta")
write_rental_stream.start()

spark.streams.awaitAnyTermination()