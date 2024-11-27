import logging
from datetime import datetime, UTC
from fastapi import status
from RSKafkaWrapper.client import KafkaClient


class RSKafkaException(Exception):
    def __init__(
            self,
            message: str,
            code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
            kafka_client: KafkaClient = None,
            topic: str = None
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.kafka_client = kafka_client
        self.topic = topic
        logging.error(f"RSKafkaException: {message} (Status Code: {code})")
        if kafka_client and topic:
            self.send_error_to_kafka()

    def send_error_to_kafka(self) -> None:
        """Send error message to Kafka topic."""
        error_message = {
            "status_code": self.code,
            "message": self.message,
            "timestamp": datetime.now(UTC).isoformat()
        }
        self.kafka_client.send_message(self.topic, error_message)

    def __str__(self) -> str:
        return f"RSKafkaException: {self.message} (Status Code: {self.code})"
