from functools import wraps


class KafkaTopicUtils:
    @staticmethod
    def infer_topic_name(service_method_name: str) -> str:
        """
        Infers Kafka topic name from service method name.
        Example: 'register_device_service' -> 'register_device_response'
        """
        # Remove '_service' suffix and add '_response'
        if service_method_name.endswith('_service'):
            base_name = service_method_name[:-8]  # Remove '_service'
            return f"{base_name}_response"
        return f"{service_method_name}_response"


def infer_kafka_topic(func):
    """
    Infers the topic based on the method name.
    Ex:  method name = request_service -> topic = request_response
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # Get the method name and create topic name
        method_name = func.__name__
        kafka_topic = KafkaTopicUtils.infer_topic_name(method_name)

        # Add kafka_topic to kwargs if not present
        if 'kafka_topic' not in kwargs:
            kwargs['kafka_topic'] = kafka_topic

        return func(self, *args, **kwargs)

    return wrapper
