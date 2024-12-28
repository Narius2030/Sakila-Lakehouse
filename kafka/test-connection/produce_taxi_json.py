import sys
sys.path.append("./kafka")

import csv
import os
from json import dumps
from kafka import KafkaProducer
from time import sleep
from datetime import datetime
from KafkaComponent import KafkaProducer

#set the env variable to an IP if not localhost
KAFKA_ADDRESS=os.getenv('KAFKA_ADDRESS')

# producer = KafkaProducer(bootstrap_servers=[f'160.191.244.13:9092'],
#                         key_serializer=lambda x: dumps(x).encode('utf-8'),
#                         value_serializer=lambda x: dumps(x, default=str).encode('utf-8'))

def csv_generator():
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
		# producer.send('yellow_taxi_ride.json', value=value, key=key)
		print("processing")
		yield {'value': value, 'key': key}
		sleep(1)


if __name__ == '__main__':
    avro_schema =  {
		"type": "record",
		"name": "Ride",
		"fields": [
			{"name": "vendorId", "type": "int"},
			{"name": "passenger_count", "type": "int"},
			{"name": "trip_distance", "type": "float"},
			{"name": "pickup_location", "type": "int"},
			{"name": "dropoff_location", "type": "int"},
			{"name": "payment_type", "type": "int"},
			{"name": "total_amount", "type": "float"},
			{"name": "pickup_datetime", "type": {"type":"long", "logicalType":"timestamp-millis"}}
		]
	}
    
    prod = KafkaProducer(KAFKA_ADDRESS, 'yellow_taxi_ride.json', avro_schema, csv_generator)
    prod.run()