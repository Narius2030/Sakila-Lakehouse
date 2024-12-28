import os
import json
import avro.schema
# from kafka import KafkaProducer
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.serialization import StringSerializer, AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient


KAFKA_ADDRESS = os.getenv('KAFKA_ADDRESS')

class KafkaProducer():
    def __init__(self, kafka_addr:str, topic:str, avro_schema_json=None, generator=None):
        self.kafka_addr = kafka_addr
        self.topic = topic
        # self.key = key
        self.generator = generator
        # Cấu hình Schema Registry
        self.avro_schema = avro.schema.parse(json.dumps(avro_schema_json))
        self.schema_registry_conf = {'url': f'{KAFKA_ADDRESS}:8081'} # Địa chỉ Schema Registry
        self.schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)
        # Cấu hình Producer
        self.conf = {
            'bootstrap.servers': f'{self.kafka_addr}:9092',  # Địa chỉ Kafka broker
            'client.id': 'producer-01',
            'acks': '1' # xác nhận ghi message từ leader partition
        }
        
    def run(self):
        # Khởi tạo AvroSerializer
        key_serializer = StringSerializer('utf_8')
        value_serializer = AvroSerializer(self.schema_registry_client, self.avro_schema)
        
        producer = Producer(self.conf)
        # send data to topic
        for data in self.generator():
            producer.produce(self.topic, key=data['key'], value=data['value'], key_serializer=key_serializer, value_serializer=value_serializer)

        producer.close()
    

class KafkaConsumer():
    def __init__(self, kafka_addr:str, topic:str, group_id:str=None, function=None):
        self.kafka_addr = kafka_addr
        self.topic = topic
        self.group_id = group_id
        self.function = function
        
    def run(self):
        settings = {
            'bootstrap.servers': f'{self.kafka_addr}:9092',     # Địa chỉ broker
            'group.id': self.group_id,                          # ID của consumer group
            'auto.offset.reset': 'earliest',                    # Đọc từ offset đầu tiên nếu không có offset
            'enable.auto.commit': True                          # tự động commit offset
        }
        consumer = Consumer(settings)
        consumer.subscribe([self.topic])
        
        try:
            empty_poll_count = 0  # Số lần liên tiếp không nhận được message
            max_empty_polls = 5   # Giới hạn số lần không nhận được message trước khi dừng
            
            while True:
                message = consumer.poll(1.0)
                if not message:
                    # Không nhận được message, tăng bộ đếm
                    empty_poll_count += 1
                    if empty_poll_count >= max_empty_polls:
                        print("No more messages, exiting loop.")
                        break
                    continue
                self.function(message)
                # Nhận được message, xử lý và reset bộ đếm
                empty_poll_count = 0
                
        except Exception as ex:
            print(f"An error occurred: {ex}")
        finally:
            consumer.close()
            print("Consumer closed.")

    