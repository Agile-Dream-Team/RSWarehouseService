import json
import logging
from sqlalchemy.exc import SQLAlchemyError
from app.exceptions.custom_exceptions import BadRequestException
from app.mapper.pod_mapper import dto_to_entity
from app.repository.pod_repository import PodRepository
from RSKafkaWrapper.client import KafkaClient
from RSErrorHandler.ErrorHandler import RSKafkaException


class PodService:
    def __init__(self, kafka_client: KafkaClient, db_session):
        self.kafka_client = kafka_client
        self.db_session = db_session
        self.pod_repository = PodRepository(self.db_session)

    def get_all_pods_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing message: {kafka_in_dto}")
            pods = self.pod_repository.get_all()
            pods_json = [pod.to_dict() for pod in pods]

            pods_dict = {"pods": pods_json}

            self.kafka_client.send_message("get_all_pods_response", pods_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_all_pods_response")
        finally:
            self.db_session.close()

    def save_pod_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing message: {kafka_in_dto}")
            pod = dto_to_entity(kafka_in_dto)
            self.pod_repository.save(pod)
            pod_dict = pod.to_dict()
            pod_dict.update({"status_code": 200})
            self.kafka_client.send_message("pod_response", pod_dict)
            return pod
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "pod_response")
        finally:
            self.db_session.close()

    def get_by_id_pod_service(self, kafka_in_dto):
        try:
            record_id = kafka_in_dto['id']

            pod = self.pod_repository.get_by_id(record_id)
            logging.info(f"Retrieved pod: {pod}")
            pod_dict = pod.to_dict() if pod else {}
            self.kafka_client.send_message("get_by_id_pod_response", pod_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_by_id_pod_response")
        finally:
            self.db_session.close()

    def update_pod_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing update message: {kafka_in_dto}")
            parsed = kafka_in_dto.get("data")
            pod = dto_to_entity(parsed)  # Just
            logging.info(f'{pod.to_dict()}')
            self.pod_repository.update(pod)
            pod_dict = pod.to_dict()
            pod_dict.update({"status_code": 200})
            self.kafka_client.send_message("update_pod_response", pod_dict)
            return pod
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "update_pod_response")
        finally:
            self.db_session.close()

    def delete_pod_service(self, kafka_in_dto):
        try:
            record_id = kafka_in_dto['id']
            pod = self.pod_repository.get_by_id(record_id)

            if pod:
                self.pod_repository.delete(pod)
                response_dict = {"status_code": 200, "message": f"Pod {record_id} deleted successfully"}
            else:
                response_dict = {"status_code": 404, "message": f"Pod {record_id} not found"}

            self.kafka_client.send_message("delete_pod_response", response_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "delete_pod_response")
        finally:
            self.db_session.close()

    def get_pods_by_user_service(self, kafka_in_dto):
        try:
            user_id = kafka_in_dto['user_id']
            pods = self.pod_repository.get_by_user_id(user_id)
            pods_json = [pod.to_dict() for pod in pods]

            pods_dict = {"pods": pods_json}

            self.kafka_client.send_message("get_pods_by_user_response", pods_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_pods_by_user_response")
        finally:
            self.db_session.close()

    def get_pod_with_devices_service(self, kafka_in_dto):
        try:
            record_id = kafka_in_dto['id']
            pod = self.pod_repository.get_by_id_with_devices(record_id)

            if pod:
                pod_dict = pod.to_dict()
                pod_dict['devices'] = [device.to_dict() for device in pod.devices]
            else:
                pod_dict = {}

            self.kafka_client.send_message("get_pod_with_devices_response", pod_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_pod_with_devices_response")
        finally:
            self.db_session.close()
