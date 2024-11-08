import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, status
from pydantic import BaseModel, ValidationError

from RSErrorHandler.ErrorHandler import RSKafkaException
from app.config.config import Settings
from app.config.database import setup_database
from app.services.camera_service import CameraService
from app.services.prediction_service import PredictionService
from app.services.sensor_data_service import SensorDataService
from app.services.pod_service import PodService
from app.services.device_service import DeviceService
from app.services.bucket_service import BucketService
from RSKafkaWrapper.client import KafkaClient


def configure_logging():
    logging.basicConfig(level=logging.INFO)


configure_logging()

app_settings = Settings()
app = FastAPI()

# Initialize KafkaClient using the singleton pattern
kafka_client = KafkaClient.instance(app_settings.kafka_bootstrap_servers, app_settings.kafka_group_id)

# Initialize the database and session
db_session = setup_database(app_settings)


# Dependency to get the KafkaConsumerService instance
def get_kafka_service_sensor_data():
    return SensorDataService(kafka_client, db_session)


def get_kafka_service_camera():
    return CameraService(kafka_client, db_session)


def get_kafka_service_prediction():
    return PredictionService(kafka_client, db_session)


def get_kafka_service_pod():
    return PodService(kafka_client, db_session)


def get_kafka_service_device():
    return DeviceService(kafka_client, db_session)


def get_kafka_service_bucket():
    return BucketService(kafka_client, db_session)


@asynccontextmanager
async def lifespan(fastapi_app: FastAPI):
    try:
        local_settings = Settings()
    except ValidationError as e:
        logging.error(f"Environment variable validation error: {e}")
        raise

    existing_topics = kafka_client.list_topics()
    logging.info(f"Creating Kafka topics: {local_settings.kafka_topics}")
    for topic in local_settings.kafka_topics:
        if topic not in existing_topics:
            kafka_client.create_topic(topic)
        else:
            logging.info(f"Topic '{topic}' already exists.")

    kafka_service_sensor_data = SensorDataService(kafka_client, db_session)
    kafka_service_camera = CameraService(kafka_client, db_session)
    kafka_service_pod = PodService(kafka_client, db_session)
    kafka_service_device = DeviceService(kafka_client, db_session)
    kafka_service_bucket = BucketService(kafka_client, db_session)

    logging.info("KafkaConsumerService initialized successfully.")

    # Store the services in the app's state
    fastapi_app.state.kafka_service = kafka_service_sensor_data
    fastapi_app.state.kafka_service_camera = kafka_service_camera
    fastapi_app.state.kafka_service_pod = kafka_service_pod
    fastapi_app.state.kafka_service_device = kafka_service_device
    fastapi_app.state.kafka_service_bucket = kafka_service_bucket

    yield


# Ensure the lifespan context is properly set
app.router.lifespan_context = lifespan


class HealthCheck(BaseModel):
    msg: str = "Hello world"


@app.get("/", response_model=HealthCheck, status_code=status.HTTP_200_OK)
async def get_health() -> HealthCheck:
    logging.info("Health check endpoint called")
    return HealthCheck()


@kafka_client.topic('get_all_sensor_data')
def consume_message_get_all_sensor_data(msg):
    try:
        logging.info(f"Consumed message in get_all_sensor_data: {msg}")
        kafka_service_sensor_data = get_kafka_service_sensor_data()
        kafka_service_sensor_data.get_all_sensor_data_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in save sensor data: {e}")
        raise RSKafkaException(f"Exception: {e}", kafka_client, "get_all_sensor_data_response")


@kafka_client.topic('sensor_data')
def consume_message_save_sensor_data(msg):
    try:
        logging.info(f"Consumed message in sensor_data: {msg}")
        kafka_service_sensor_data = get_kafka_service_sensor_data()
        kafka_service_sensor_data.save_sensor_data_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in save sensor data: {e}")
        raise RSKafkaException(f"Exception: {e}", kafka_client, "sensor_data_response")


@kafka_client.topic('get_by_id_sensor_data')
def consume_message_get_by_id_camera(msg):
    try:
        logging.info(f"Consumed message in get_by_id_sensor_data: {msg}")
        kafka_service_sensor_data = get_kafka_service_sensor_data()
        kafka_service_sensor_data.get_by_id_sensor_data_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_sensor_data: {e}")
        raise RSKafkaException(f"Exception: {e}", kafka_client, "get_by_id_sensor_data_response")


@kafka_client.topic('camera')
def consume_message_save_camera(msg):
    try:
        logging.info(f"Consumed message in camera: {msg}")
        kafka_service_camera = get_kafka_service_camera()
        kafka_service_camera.save_camera_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in camera: {e}")
        raise RSKafkaException(f"Exception: {e}", kafka_client, "camera_response")


@kafka_client.topic('get_all_camera')
def consume_message_get_all_camera(msg):
    try:
        logging.info(f"Consumed message in get_all_camera: {msg}")
        kafka_service_image = get_kafka_service_camera()
        kafka_service_image.get_all_camera_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_all_camera: {e}")
        raise RSKafkaException(f"Exception: {e}", kafka_client, "get_all_camera_response")


@kafka_client.topic('get_by_id_camera')
def consume_message_get_by_id_camera(msg):
    try:
        logging.info(f"Consumed message in get_by_id_camera: {msg}")
        kafka_service_image = get_kafka_service_camera()
        kafka_service_image.get_by_id_camera_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_camera: {e}")
        raise RSKafkaException(f"Exception: {e}", kafka_client, "get_by_id_camera_response")


@kafka_client.topic('prediction_request')
def consume_message_get_prediction(msg):
    try:
        logging.info(f"Consumed message in prediction_request: {msg}")
        kafka_service_prediction = get_kafka_service_prediction()
        kafka_service_prediction.get_prediction_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in prediction_request: {e}")
        raise RSKafkaException(f"Exception: {e}", kafka_client, "prediction_response")


@kafka_client.topic('live_stream')
def consume_message_get_prediction(msg):
    try:
        logging.info(f"Consumed message in prediction_request: {msg}")
    except Exception as e:
        logging.error(f"Error processing message in prediction_request: {e}")


@kafka_client.topic('get_all_pods')
def consume_message_get_all_pods(msg):
    try:
        logging.info(f"Consumed message in get_all_pods: {msg}")
        kafka_service_pod = get_kafka_service_pod()
        kafka_service_pod.get_all_pods_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_all_pods: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_all_pods_response")


@kafka_client.topic('pod')
def consume_message_save_pod(msg):
    try:
        logging.info(f"Consumed message in pod: {msg}")
        kafka_service_pod = get_kafka_service_pod()
        kafka_service_pod.save_pod_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in pod: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "pod_response")


@kafka_client.topic('get_by_id_pod')
def consume_message_get_by_id_pod(msg):
    try:
        logging.info(f"Consumed message in get_by_id_pod: {msg}")
        kafka_service_pod = get_kafka_service_pod()
        kafka_service_pod.get_by_id_pod_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_pod: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_by_id_pod_response")


@kafka_client.topic('update_pod')
def consume_message_update_pod(msg):
    try:
        logging.info(f"Consumed message in update_pod: {msg}")
        kafka_service_pod = get_kafka_service_pod()
        kafka_service_pod.update_pod_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in update_pod: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "update_pod_response")


@kafka_client.topic('delete_pod')
def consume_message_delete_pod(msg):
    try:
        logging.info(f"Consumed message in delete_pod: {msg}")
        kafka_service_pod = get_kafka_service_pod()
        kafka_service_pod.delete_pod_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in delete_pod: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "delete_pod_response")


# Device Kafka consumers
@kafka_client.topic('get_all_devices')
def consume_message_get_all_devices(msg):
    try:
        logging.info(f"Consumed message in get_all_devices: {msg}")
        kafka_service_device = get_kafka_service_device()
        kafka_service_device.get_all_devices_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_all_devices: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_all_devices_response")


@kafka_client.topic('device')
def consume_message_save_device(msg):
    try:
        logging.info(f"Consumed message in device: {msg}")
        kafka_service_device = get_kafka_service_device()
        kafka_service_device.save_device_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in device: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "device_response")


@kafka_client.topic('get_by_id_device')
def consume_message_get_by_id_device(msg):
    try:
        logging.info(f"Consumed message in get_by_id_device: {msg}")
        kafka_service_device = get_kafka_service_device()
        kafka_service_device.get_by_id_device_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_device: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_by_id_device_response")


@kafka_client.topic('update_device')
def consume_message_update_device(msg):
    try:
        logging.info(f"Consumed message in update_device: {msg}")
        kafka_service_device = get_kafka_service_device()
        kafka_service_device.update_device_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in update_device: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "update_device_response")


@kafka_client.topic('delete_device')
def consume_message_delete_device(msg):
    try:
        logging.info(f"Consumed message in delete_device: {msg}")
        kafka_service_device = get_kafka_service_device()
        kafka_service_device.delete_device_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in delete_device: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "delete_device_response")


@kafka_client.topic('get_all_buckets')
def consume_message_get_all_buckets(msg):
    try:
        logging.info(f"Consumed message in get_all_buckets: {msg}")
        kafka_service_bucket = get_kafka_service_bucket()
        kafka_service_bucket.get_all_buckets_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_all_buckets: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_all_buckets_response")


@kafka_client.topic('bucket')
def consume_message_save_bucket(msg):
    try:
        logging.info(f"Consumed message in bucket: {msg}")
        kafka_service_bucket = get_kafka_service_bucket()
        kafka_service_bucket.save_bucket_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in bucket: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "bucket_response")


@kafka_client.topic('get_by_id_bucket')
def consume_message_get_by_id_bucket(msg):
    try:
        logging.info(f"Consumed message in get_by_id_bucket: {msg}")
        kafka_service_bucket = get_kafka_service_bucket()
        kafka_service_bucket.get_by_id_bucket_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_by_id_bucket: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_by_id_bucket_response")


@kafka_client.topic('update_bucket')
def consume_message_update_bucket(msg):
    try:
        logging.info(f"Consumed message in update_bucket: {msg}")
        kafka_service_bucket = get_kafka_service_bucket()
        kafka_service_bucket.update_bucket_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in update_bucket: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "update_bucket_response")


@kafka_client.topic('delete_bucket')
def consume_message_delete_bucket(msg):
    try:
        logging.info(f"Consumed message in delete_bucket: {msg}")
        kafka_service_bucket = get_kafka_service_bucket()
        kafka_service_bucket.delete_bucket_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in delete_bucket: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "delete_bucket_response")


@kafka_client.topic('get_devices_by_pod')
def consume_message_get_devices_by_pod(msg):
    try:
        logging.info(f"Consumed message in get_devices_by_pod: {msg}")
        kafka_service_device = get_kafka_service_device()
        kafka_service_device.get_devices_by_pod_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_devices_by_pod: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_devices_by_pod_response")


@kafka_client.topic('get_buckets_by_device')
def consume_message_get_buckets_by_device(msg):
    try:
        logging.info(f"Consumed message in get_buckets_by_device: {msg}")
        kafka_service_bucket = get_kafka_service_bucket()
        kafka_service_bucket.get_buckets_by_device_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_buckets_by_device: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_buckets_by_device_response")


@kafka_client.topic('get_pod_with_devices')
def consume_message_get_pod_with_devices(msg):
    try:
        logging.info(f"Consumed message in get_pod_with_devices: {msg}")
        kafka_service_pod = get_kafka_service_pod()
        kafka_service_pod.get_pod_with_devices_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_pod_with_devices: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_pod_with_devices_response")


@kafka_client.topic('get_device_with_buckets')
def consume_message_get_device_with_buckets(msg):
    try:
        logging.info(f"Consumed message in get_device_with_buckets: {msg}")
        kafka_service_device = get_kafka_service_device()
        kafka_service_device.get_device_with_buckets_service(msg)
    except Exception as e:
        logging.error(f"Error processing message in get_device_with_buckets: {e}")
        RSKafkaException(f"Exception: {e}", kafka_client, "get_device_with_buckets_response")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=app_settings.webhook_host,
        port=app_settings.webhook_port,
        reload=app_settings.environment == 'dev'
    )
