from kafka import KafkaProducer
from json import dumps
import os
from dotenv import load_dotenv
load_dotenv()

KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")


def send_message(message):
    print("Kafka Producer Application Started ... ")
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                             api_version=(0, 11, 5),
                             value_serializer=lambda x: dumps(x).encode('utf-8'))
    print("Sending message: ", message)
    producer.send(KAFKA_TOPIC_NAME, message)
