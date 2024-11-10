import json
import uuid
import os
import json
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaProducer
from faker import Faker
from time import sleep
from datetime import datetime, timedelta

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

producer = KafkaProducer(bootstrap_servers=f"{kafka_host}:9092")
faker = Faker()


class DataGenerator(object):
    @staticmethod
    def get_data():
        return [
            faker.random_int(100,1000),
            faker.random_int(10, 1000),
            datetime.now().isoformat()
        ]
        
while True:
        columns = [
            "purchase_id",
            "amount",
            "timestamp",
            ]
        data_list = DataGenerator.get_data()
        json_data = dict(zip(columns, data_list))
        _payload = json.dumps(json_data).encode("utf-8")
        print(_payload, flush=True)
        print("=-" * 5, flush=True)
        response = producer.send(topic=kafka_topic, value=_payload)
        print(response.get())
        print("=-" * 20, flush=True)
        sleep(3)