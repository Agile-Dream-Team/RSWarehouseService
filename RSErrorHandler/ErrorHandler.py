import json
import logging
from RSKafkaWrapper.client import KafkaClient


class RSKafkaException(Exception):
    def __init__(self, message, kafka_client: KafkaClient, topic: str):
        super().__init__(message)
        self.message = message
        self.kafka_client = kafka_client
        self.topic = topic
        logging.error(f"RSKafkaException: {message}")
        self.send_error_to_kafka()

    def send_error_to_kafka(self):
        error_message = {"status_code": 400, "error": self.message}
        self.kafka_client.send_message(self.topic, error_message)
