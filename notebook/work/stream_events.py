import sys
sys.path.append("./work/streamify")

from utils.config import get_settings
from operators.streaming import SparkStreaming
from utils.streaming_functions import rental_process_stream, payment_process_stream
from utils.schema import *

settings = get_settings()
PAYMENT_TOPIC = "dbserver1.public.payment"
RENTAL_TOPIC = "dbserver1.public.rental"

RENTAL_STORAGE_PATH = "s3a://lakehouse/streamify.db/rental"
PAYMENT_STORAGE_PATH = "s3a://lakehouse/streamify.db/payment"

RENTAL_CHECKPOINT_PATH = "s3a://lakehouse/streaming/streamify/checkpoints/checkpoint_rental"
PAYMENT_CHECKPOINT_PATH = "s3a://lakehouse/streaming/streamify/checkpoints/checkpoint_payment"

## TODO: create a SparkSession
spark = SparkStreaming.get_instance(app_name="Streamify Spark Streaming")

## TODO: create stream reader
rental_stream = SparkStreaming.create_kafka_read_stream(spark, settings.KAFKA_ADDRESS, settings.KAFKA_PORT, RENTAL_TOPIC)
payment_stream = SparkStreaming.create_kafka_read_stream(spark, settings.KAFKA_ADDRESS, settings.KAFKA_PORT, PAYMENT_TOPIC)

## TODO: process stream
processed_rental = rental_process_stream(rental_stream, schema_rental)
processed_payment = payment_process_stream(payment_stream, schema_payment)

## TODO: write stream into Delta Lake
write_rental_stream = SparkStreaming.create_file_write_stream(processed_rental, RENTAL_CHECKPOINT_PATH, RENTAL_STORAGE_PATH, file_format="delta", partitions="rental_year")
write_rental_stream.start()

write_payment_stream = SparkStreaming.create_file_write_stream(processed_payment, PAYMENT_CHECKPOINT_PATH, PAYMENT_STORAGE_PATH, file_format="delta", partitions="payment_year")
write_payment_stream.start()

spark.streams.awaitAnyTermination()