import logging
from sqlalchemy.exc import SQLAlchemyError
from app.mapper.bucket_mapper import dto_to_entity
from app.repository.bucket_repository import BucketRepository
from RSKafkaWrapper.client import KafkaClient
from RSErrorHandler.ErrorHandler import RSKafkaException


class BucketService:
    def __init__(self, kafka_client: KafkaClient, db_session):
        self.kafka_client = kafka_client
        self.db_session = db_session
        self.bucket_repository = BucketRepository(self.db_session)

    def get_all_buckets_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing message: {kafka_in_dto}")
            buckets = self.bucket_repository.get_all()
            buckets_json = [bucket.to_dict() for bucket in buckets]

            buckets_dict = {"buckets": buckets_json}

            self.kafka_client.send_message("get_all_buckets_response", buckets_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_all_buckets_response")
        finally:
            self.db_session.close()

    def save_bucket_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing message: {kafka_in_dto}")
            bucket = dto_to_entity(kafka_in_dto)
            self.bucket_repository.save(bucket)
            bucket_dict = bucket.to_dict()
            bucket_dict.update({"status_code": 200})
            self.kafka_client.send_message("bucket_response", bucket_dict)
            return bucket
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "bucket_response")
        finally:
            self.db_session.close()

    def get_by_id_bucket_service(self, kafka_in_dto):
        try:
            record_id = kafka_in_dto['id']

            bucket = self.bucket_repository.get_by_id(record_id)
            logging.info(f"Retrieved bucket: {bucket}")
            bucket_dict = bucket.to_dict() if bucket else {}
            self.kafka_client.send_message("get_by_id_bucket_response", bucket_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_by_id_bucket_response")
        finally:
            self.db_session.close()

    def update_bucket_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing update message: {kafka_in_dto}")
            parsed = kafka_in_dto.get("data")
            bucket = dto_to_entity(parsed)  # Just
            logging.info(f'{bucket.to_dict()}')
            self.bucket_repository.update(bucket)
            pod_dict = bucket.to_dict()
            pod_dict.update({"status_code": 200})
            self.kafka_client.send_message("update_bucket_response", pod_dict)
            return bucket
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "update_bucket_response")
        finally:
            self.db_session.close()

    def delete_bucket_service(self, kafka_in_dto):
        try:
            record_id = kafka_in_dto['id']
            bucket = self.bucket_repository.get_by_id(record_id)

            if bucket:
                self.bucket_repository.delete(bucket)
                response_dict = {"status_code": 200, "message": f"Bucket {record_id} deleted successfully"}
            else:
                response_dict = {"status_code": 404, "message": f"Bucket {record_id} not found"}

            self.kafka_client.send_message("delete_bucket_response", response_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "delete_bucket_response")
        finally:
            self.db_session.close()

    def get_buckets_by_device_service(self, kafka_in_dto):
        try:
            logging.info(f"Processing message: {kafka_in_dto}")
            device_id = kafka_in_dto['device_id']

            buckets = self.bucket_repository.get_buckets_by_device_id(device_id)
            buckets_json = [bucket.to_dict() for bucket in buckets]

            buckets_dict = {"buckets": buckets_json}

            self.kafka_client.send_message("get_buckets_by_device_response", buckets_dict)
        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_buckets_by_device_response")
        finally:
            self.db_session.close()

