import csv
import os
from json import dumps
from kafka import KafkaProducer
from time import sleep
from datetime import datetime

#set the env variable to an IP if not localhost
KAFKA_ADDRESS=os.getenv('KAFKA_ADDRESS')

producer = KafkaProducer(bootstrap_servers=[f'160.191.244.13:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x, default=str).encode('utf-8'))

file = open('./kafka/test-connection/data/rides.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"vendorId": int(row[0])}
    
    value = {"vendorId": int(row[0]),
            "passenger_count": int(row[3]),
            "trip_distance": float(row[4]),
            "pickup_location": int(row[7]),
            "dropoff_location": int(row[8]),
            "payment_type": int(row[9]),
            "total_amount": float(row[16]),
            "pickup_datetime": datetime.now()
            }

    producer.send('yellow_taxi_ride.json', value=value, key=key)
    print("producing")
    sleep(3)
