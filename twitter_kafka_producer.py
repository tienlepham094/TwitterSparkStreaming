from kafka import KafkaProducer
import json

from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

KAFKA_TOPIC_NAME_CONS = "twitter_streaming"
KAFKA_BOOTSTRAP_SERVERS_CONS = '127.0.0.1:9092'

def send_message(message):
    print("Kafka Producer Application Started ... ")
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                            api_version=(0,11,5),
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    print("Sending message: ", message)
    producer.send(KAFKA_TOPIC_NAME_CONS, message)
    

        