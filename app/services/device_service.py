import logging
from datetime import datetime, UTC
from sqlalchemy.exc import SQLAlchemyError
from app.mapper.device_mapper import dto_to_entity
from app.repository.device_repository import DeviceRepository
from app.repository.esp32_repository import ESP32Repository
from RSKafkaWrapper.client import KafkaClient
from RSErrorHandler.ErrorHandler import RSKafkaException
from app.utils.db_utils import transaction_scope
from app.utils.kafka_utils import infer_kafka_topic
from app.utils.response_utils import create_response
from fastapi import status


class DeviceService:
    def __init__(self, kafka_client: KafkaClient, db_session):
        self.kafka_client = kafka_client
        self.db_session = db_session
        self.device_repository = DeviceRepository(self.db_session)
        self.esp32_repository = ESP32Repository(self.db_session)

    @infer_kafka_topic
    def create_device_service(self, kafka_in_dto, kafka_topic: str = None):
        """
        Register a new device with optional ESP32 devices.

        Args:
            kafka_in_dto: Dictionary containing device registration details
            kafka_topic: Kafka topic for response messages
        """
        try:
            logging.info(f"Processing device registration: {kafka_in_dto}")

            with transaction_scope(self.db_session, self.kafka_client, kafka_topic):
                # Create and save main device
                device = dto_to_entity(kafka_in_dto)
                device.device_type = 'raspberry_pi'
                device.status = 'active'
                device.last_seen = datetime.now(UTC)
                saved_device = self.device_repository.save(device)

                # Process ESP32 devices if any
                esp_devices = []
                if 'esp_devices' in kafka_in_dto:
                    for esp_data in kafka_in_dto['esp_devices']:
                        esp_device = self.esp32_repository.create_esp32(
                            device_id=saved_device.id,
                            esp_id=esp_data['esp_id'],
                            name=esp_data['name'],
                            location=esp_data['location'],
                            capabilities=esp_data['capabilities'],
                            device_metadata=esp_data.get('metadata', {}),
                            user_id=saved_device.user_id
                        )
                        esp_devices.append(esp_device)

                # Prepare and send response
                response = create_response(
                    status_code=status.HTTP_200_OK,
                    message="Device registered successfully",
                    data={
                        **saved_device.to_dict(),
                        "esp_devices": [esp.to_dict() for esp in esp_devices]
                    }
                )

                self.kafka_client.send_message(kafka_topic, response)

        except Exception as e:
            error_msg = f"Error registering device: {str(e)}"
            #logging.error(error_msg)
            RSKafkaException(
                message=error_msg,
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                kafka_client=self.kafka_client,
                topic=kafka_topic
            )

    """
    def update_device_status_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto['device_id']
            new_status = kafka_in_dto['status']

            device = self.device_repository.update_status(device_id, new_status)

            response_dict = device.to_dict()
            response_dict['status_code'] = 200

            self.kafka_client.send_message("device_status_update_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "device_status_update_response")
        finally:
            self.db_session.close()
    """

    """
    def get_device_with_esp32s_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto['device_id']

            device = self.device_repository.get_by_id_with_esp32s(device_id)

            if device:
                response_dict = device.to_dict()
                response_dict['esp_devices'] = [esp.to_dict() for esp in device.esp_devices]
            else:
                response_dict = {}

            self.kafka_client.send_message("get_device_with_esp32s_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_device_with_esp32s_response")
        finally:
            self.db_session.close()
    """

    """
    def assign_esp32_to_bucket_service(self, kafka_in_dto):
        try:
            esp_id = kafka_in_dto['esp_id']
            bucket_id = kafka_in_dto['bucket_id']

            esp_device = self.esp32_repository.assign_to_bucket(esp_id, bucket_id)

            response_dict = esp_device.to_dict()
            response_dict['status_code'] = 200

            self.kafka_client.send_message("esp32_bucket_assignment_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "esp32_bucket_assignment_response")
        finally:
            self.db_session.close()
    """

    @infer_kafka_topic
    def get_active_devices_service(self, kafka_topic: str = None):
        """
        Get all active devices with their ESP32 devices.

        Args:
            kafka_topic: Kafka topic for response messages
        """
        try:
            logging.info("Retrieving active devices")

            with transaction_scope(self.db_session, self.kafka_client, kafka_topic):
                devices = self.device_repository.get_active_devices()

                response = create_response(
                    status_code=status.HTTP_200_OK,
                    message="Active devices retrieved successfully",
                    data={
                        "devices": [
                            {
                                **device.to_dict(),
                                "esp_devices": [esp.to_dict() for esp in device.esp_devices]
                            }
                            for device in devices
                        ]
                    }
                )
                self.kafka_client.send_message(kafka_topic, response)

        except Exception as e:
            error_msg = f"Error retrieving active devices: {str(e)}"
            logging.error(error_msg)
            RSKafkaException(
                message=error_msg,
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                kafka_client=self.kafka_client,
                topic=kafka_topic
            )

    """
    def get_all_devices_service(self):
        try:
            devices = self.device_repository.get_all()
            devices_json = [
                {
                    **device.to_dict(),
                    "esp_devices": [esp.to_dict() for esp in device.esp_devices]
                }
                for device in devices
            ]

            self.kafka_client.send_message("get_all_devices_response", {"devices": devices_json})

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_all_devices_response")
        finally:
            self.db_session.close()
    """
    """
    # Add ESP32-specific services
    def update_esp32_status_service(self, kafka_in_dto):
        try:
            esp_id = kafka_in_dto['esp_id']
            new_status = kafka_in_dto['status']

            esp_device = self.esp32_repository.update_status(esp_id, new_status)

            response_dict = esp_device.to_dict()
            response_dict['status_code'] = 200

            self.kafka_client.send_message("esp32_status_update_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "esp32_status_update_response")
        finally:
            self.db_session.close()

    def get_device_statistics_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto.get('device_id')

            if device_id:
                stats = self.device_repository.get_device_statistics(device_id)
            else:
                stats = self.device_repository.get_all_devices_statistics()

            self.kafka_client.send_message("device_statistics_response", {"statistics": stats})

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "device_statistics_response")
        finally:
            self.db_session.close()

    def get_esp32_by_bucket_service(self, kafka_in_dto):
        try:
            bucket_id = kafka_in_dto['bucket_id']

            esp_devices = self.esp32_repository.get_by_bucket_id(bucket_id)
            response_dict = {
                "esp_devices": [esp.to_dict() for esp in esp_devices]
            }

            self.kafka_client.send_message("get_esp32_by_bucket_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_esp32_by_bucket_response")
        finally:
            self.db_session.close()

    def update_device_metadata_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto['device_id']
            device_metadata = kafka_in_dto['device_metadata']

            device = self.device_repository.update_metadata(device_id, device_metadata)
            response_dict = device.to_dict()
            response_dict['status_code'] = 200

            self.kafka_client.send_message("update_device_metadata_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "update_device_metadata_response")
        finally:
            self.db_session.close()

    def update_esp32_metadata_service(self, kafka_in_dto):
        try:
            esp_id = kafka_in_dto['esp_id']
            device_metadata = kafka_in_dto['device_metadata']

            esp_device = self.esp32_repository.update_metadata(esp_id, device_metadata)
            response_dict = esp_device.to_dict()
            response_dict['status_code'] = 200

            self.kafka_client.send_message("update_esp32_metadata_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "update_esp32_metadata_response")
        finally:
            self.db_session.close()

    def get_device_heartbeat_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto['device_id']
            status = kafka_in_dto.get('status', 'active')

            device = self.device_repository.update_heartbeat(device_id, status)

            # Also update ESP32 devices if they're included in the heartbeat
            if 'esp_devices' in kafka_in_dto:
                for esp_data in kafka_in_dto['esp_devices']:
                    self.esp32_repository.update_heartbeat(
                        esp_data['esp_id'],
                        esp_data.get('status', 'active')
                    )

            response_dict = {
                "status": "success",
                "device_id": device_id,
                "last_seen": device.last_seen.isoformat() if device else None,
                "status_code": 200
            }

            self.kafka_client.send_message("device_heartbeat_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "device_heartbeat_response")
        finally:
            self.db_session.close()

    def delete_esp32_service(self, kafka_in_dto):
        try:
            esp_id = kafka_in_dto['esp_id']

            success = self.esp32_repository.delete_by_esp_id(esp_id)

            response_dict = {
                "status_code": 200 if success else 404,
                "message": f"ESP32 {esp_id} {'deleted successfully' if success else 'not found'}"
            }

            self.kafka_client.send_message("delete_esp32_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "delete_esp32_response")
        finally:
            self.db_session.close()

    def get_inactive_devices_service(self, kafka_in_dto):
        try:
            threshold_minutes = kafka_in_dto.get('threshold_minutes', 5)

            inactive_devices = self.device_repository.get_inactive_devices(threshold_minutes)
            response_dict = {
                "devices": [
                    {
                        **device.to_dict(),
                        "esp_devices": [
                            esp.to_dict() for esp in device.esp_devices
                            if esp.status == 'inactive'
                        ]
                    }
                    for device in inactive_devices
                ]
            }

            self.kafka_client.send_message("get_inactive_devices_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_inactive_devices_response")
        finally:
            self.db_session.close()

    def validate_device_configuration_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto['device_id']

            device = self.device_repository.get_by_id_with_esp32s(device_id)
            if not device:
                raise ValueError(f"Device not found: {device_id}")

            validation_result = {
                "device_id": device_id,
                "name": device.name,
                "status": device.status,
                "ip_address": device.ip_address,
                "port": device.port,
                "is_valid": True,
                "issues": [],
                "esp_devices": []
            }

            # Validate device configuration
            if not device.ip_address or not device.port:
                validation_result["is_valid"] = False
                validation_result["issues"].append("Missing IP address or port")

            # Validate ESP32 devices
            for esp in device.esp_devices:
                esp_validation = {
                    "esp_id": esp.esp_id,
                    "name": esp.name,
                    "is_valid": True,
                    "issues": []
                }

                if not esp.capabilities:
                    esp_validation["is_valid"] = False
                    esp_validation["issues"].append("No capabilities defined")

                validation_result["esp_devices"].append(esp_validation)
                if not esp_validation["is_valid"]:
                    validation_result["is_valid"] = False

            self.kafka_client.send_message("device_validation_response", validation_result)

        except Exception as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Validation error: {e}", self.kafka_client, "device_validation_response")
        finally:
            self.db_session.close()

    def sync_device_configuration_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto['device_id']
            new_configuration = kafka_in_dto['configuration']

            # Get existing device with ESP32s
            device = self.device_repository.get_by_id_with_esp32s(device_id)
            if not device:
                raise ValueError(f"Device not found: {device_id}")

            # Update device configuration
            device = self.device_repository.update_device_configuration(
                device_id,
                new_configuration.get('ip_address'),
                new_configuration.get('port'),
                new_configuration.get('name'),
                new_configuration.get('location'),
                new_configuration.get('device_metadata', {})
            )

            # Handle ESP32 configurations
            esp32_updates = []
            if 'esp_devices' in new_configuration:
                for esp_config in new_configuration['esp_devices']:
                    esp_device = self.esp32_repository.sync_esp32_configuration(
                        device.id,
                        esp_config['esp_id'],
                        esp_config.get('name'),
                        esp_config.get('location'),
                        esp_config.get('capabilities', []),
                        esp_config.get('device_metadata', {})
                    )
                    esp32_updates.append(esp_device)

            response_dict = {
                "status_code": 200,
                "device": device.to_dict(),
                "esp_devices": [esp.to_dict() for esp in esp32_updates]
            }

            self.kafka_client.send_message("device_sync_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "device_sync_response")
        finally:
            self.db_session.close()

    def get_device_by_serial_service(self, kafka_in_dto):
        try:
            serial = kafka_in_dto['serial']

            device = self.device_repository.get_by_serial_with_esp32s(serial)
            if device:
                response_dict = {
                    **device.to_dict(),
                    "esp_devices": [esp.to_dict() for esp in device.esp_devices]
                }
            else:
                response_dict = {}

            self.kafka_client.send_message("get_device_by_serial_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "get_device_by_serial_response")
        finally:
            self.db_session.close()

    def bulk_update_esp32_status_service(self, kafka_in_dto):
        try:
            updates = kafka_in_dto['updates']
            results = []

            for update in updates:
                esp_device = self.esp32_repository.update_status(
                    update['esp_id'],
                    update['status']
                )
                results.append({
                    "esp_id": update['esp_id'],
                    "status": esp_device.status if esp_device else "not_found",
                    "success": esp_device is not None
                })

            response_dict = {
                "status_code": 200,
                "results": results
            }

            self.kafka_client.send_message("bulk_esp32_status_update_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "bulk_esp32_status_update_response")
        finally:
            self.db_session.close()

    def get_device_summary_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto.get('device_id')

            if device_id:
                summary = self.device_repository.get_device_summary(device_id)
            else:
                summary = self.device_repository.get_all_devices_summary()

            response_dict = {
                "status_code": 200,
                "summary": summary
            }

            self.kafka_client.send_message("device_summary_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "device_summary_response")
        finally:
            self.db_session.close()

    def migrate_device_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto['device_id']
            new_pod_id = kafka_in_dto['new_pod_id']

            device = self.device_repository.migrate_device(device_id, new_pod_id)

            response_dict = {
                "status_code": 200,
                "device": device.to_dict(),
                "message": f"Device successfully migrated to pod {new_pod_id}"
            }

            self.kafka_client.send_message("device_migration_response", response_dict)

        except SQLAlchemyError as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Database error: {e}", self.kafka_client, "device_migration_response")
        finally:
            self.db_session.close()
    
    def get_device_health_check_service(self, kafka_in_dto):
        try:
            device_id = kafka_in_dto['device_id']

            device = self.device_repository.get_by_id_with_esp32s(device_id)
            if not device:
                raise ValueError(f"Device not found: {device_id}")

            health_status = {
                "device_id": device_id,
                "name": device.name,
                "status": device.status,
                "last_seen": device.last_seen.isoformat() if device.last_seen else None,
                "ip_address": device.ip_address,
                "port": device.port,
                "esp_devices": [
                    {
                        "esp_id": esp.esp_id,
                        "name": esp.name,
                        "status": esp.status,
                        "last_seen": esp.last_seen.isoformat() if esp.last_seen else None,
                        "capabilities": esp.capabilities
                    }
                    for esp in device.esp_devices
                ]
            }

            self.kafka_client.send_message("device_health_check_response", health_status)

        except Exception as e:
            self.db_session.rollback()
            raise RSKafkaException(f"Health check error: {e}", self.kafka_client, "device_health_check_response")
        finally:
            self.db_session.close()
    """

    @infer_kafka_topic
    def get_by_id_device_service(self, kafka_in_dto, kafka_topic: str = None):
        """Get device by ID and send response through Kafka."""

        try:
            record_id = kafka_in_dto['id']
            logging.info(f"Attempting to retrieve device with ID: {record_id}")

            with transaction_scope(self.db_session, self.kafka_client, kafka_topic):
                device = self.device_repository.get_by_id(record_id)

                if device:
                    logging.info(f"Successfully retrieved device: {device}")
                    response = create_response(
                        status_code=status.HTTP_200_OK,
                        message="Device retrieved successfully",
                        data=device.to_dict()
                    )
                    self.kafka_client.send_message(kafka_topic, response)
                else:
                    logging.warning(f"Device with ID {record_id} not found")
                    error_msg = f"id: {record_id} not found"
                    RSKafkaException(
                        message=error_msg,
                        code=status.HTTP_404_NOT_FOUND,
                        kafka_client=self.kafka_client,
                        topic=kafka_topic
                    )

        except Exception as e:
            error_msg = f"Error retrieving device: {str(e)}"
            logging.error(error_msg)
            RSKafkaException(
                message=error_msg,
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                kafka_client=self.kafka_client,
                topic=kafka_topic
            )

    @infer_kafka_topic
    def delete_device_service(self, kafka_in_dto,  kafka_topic: str = None):
        """Delete a device and send response through Kafka."""
        try:
            record_id = kafka_in_dto['id']
            logging.info(f"Attempting to delete device with ID: {record_id}")

            with transaction_scope(self.db_session, self.kafka_client, kafka_topic):
                device = self.device_repository.get_by_id(record_id)

                if device:
                    self.device_repository.delete(device)
                    response = create_response(
                        status_code=status.HTTP_200_OK,
                        message="Device deleted successfully"
                    )
                    self.kafka_client.send_message(kafka_topic, response)
                else:
                    error_msg = f"id: {record_id} not found"
                    RSKafkaException(
                        message=error_msg,
                        code=status.HTTP_404_NOT_FOUND,
                        kafka_client=self.kafka_client,
                        topic=kafka_topic
                    )

                self.kafka_client.send_message(kafka_topic, response)

        except Exception as e:
            error_msg = f"Error deleting device: {str(e)}"
            logging.error(error_msg)
            RSKafkaException(
                message=error_msg,
                code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                kafka_client=self.kafka_client,
                topic=kafka_topic
            )
