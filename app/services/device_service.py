import logging
from sqlalchemy.exc import SQLAlchemyError
from app.mapper.device_mapper import dto_to_entity
from app.repository.device_repository import DeviceRepository
from RSKafkaWrapper.client import KafkaClient
from RSErrorHandler.ErrorHandler import RSKafkaException


class DeviceService:
    def __init__(self, kafka_client: KafkaClient, db_session):
        self.kafka_client = kafka_client
        self.db_session = db_session
        self.device_repository = DeviceRepository(self.db_session)

    def get_all_devices_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing message: {kafka_in_dto}")
            devices = self.device_repository.get_all()
            devices_json = [device.to_dict() for device in devices]

            devices_dict = {"devices": devices_json}

            self.kafka_client.send_message("get_all_devices_response", devices_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_all_devices_response")
        finally:
            self.db_session.close()

    def save_device_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing message: {kafka_in_dto}")
            device = dto_to_entity(kafka_in_dto)
            self.device_repository.save(device)
            device_dict = device.to_dict()
            device_dict.update({"status_code": 200})
            self.kafka_client.send_message("device_response", device_dict)
            return device
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "device_response")
        finally:
            self.db_session.close()

    def get_by_id_device_service(self, kafka_in_dto):
        try:
            record_id = kafka_in_dto['id']

            device = self.device_repository.get_by_id(record_id)
            logging.info(f"Retrieved device: {device}")
            device_dict = device.to_dict() if device else {}
            self.kafka_client.send_message("get_by_id_device_response", device_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_by_id_device_response")
        finally:
            self.db_session.close()

    def update_device_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing update message: {kafka_in_dto}")
            parsed = kafka_in_dto.get("data")
            device = dto_to_entity(parsed)  # Just
            logging.info(f'{device.to_dict()}')
            self.device_repository.update(device)
            pod_dict = device.to_dict()
            pod_dict.update({"status_code": 200})
            self.kafka_client.send_message("update_device_response", pod_dict)
            return device
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "update_device_response")
        finally:
            self.db_session.close()

    def delete_device_service(self, kafka_in_dto):
        try:
            record_id = kafka_in_dto['id']
            device = self.device_repository.get_by_id(record_id)

            if device:
                self.device_repository.delete(device)
                response_dict = {"status_code": 200, "message": f"device {record_id} deleted successfully"}
            else:
                response_dict = {"status_code": 404, "message": f"device {record_id} not found"}

            self.kafka_client.send_message("delete_device_response", response_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "delete_device_response")
        finally:
            self.db_session.close()

    def get_devices_by_pod_service(self, kafka_in_dto):
        try:
            pod_id = kafka_in_dto['pod_id']
            devices = self.device_repository.get_by_pod_id(pod_id)
            devices_json = [device.to_dict() for device in devices]

            devices_dict = {"devices": devices_json}

            self.kafka_client.send_message("get_devices_by_pod_response", devices_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_devices_by_pod_response")
        finally:
            self.db_session.close()

    def get_device_with_buckets_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing message: {kafka_in_dto}")
            record_id = kafka_in_dto['id']

            device = self.device_repository.get_by_id_with_buckets(record_id)
            logging.info(f"Retrieved device with buckets: {device}")

            if device:
                device_dict = device.to_dict()
                device_dict['buckets'] = [bucket.to_dict() for bucket in device.buckets]
            else:
                device_dict = {}

            self.kafka_client.send_message("get_device_with_buckets_response", device_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_device_with_buckets_response")
        finally:
            self.db_session.close()
