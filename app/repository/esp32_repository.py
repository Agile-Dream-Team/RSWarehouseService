# app/repository/esp32_repository.py
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from datetime import datetime, UTC

from app.domain.device import Device
from app.domain.esp32_device import ESP32Device


class ESP32Repository:
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def create_esp32(self, device_id: int, esp_id: str, name: str,
                     location: str, capabilities: list, device_metadata: dict,
                     user_id: int) -> ESP32Device:
        try:
            esp_device = ESP32Device(
                device_id=device_id,
                esp_id=esp_id,
                name=name,
                location=location,
                capabilities=capabilities,
                device_metadata=device_metadata,
                user_id=user_id,  # This is required
                status='active',
                last_seen=datetime.now(UTC)
            )
            self.db_session.add(esp_device)
            self.db_session.commit()
            return esp_device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def get_by_esp_id(self, esp_id: str):
        return self.db_session.query(ESP32Device) \
            .filter(ESP32Device.esp_id.__eq__(esp_id)) \
            .first()

    def get_by_device_id(self, device_id: int):
        return self.db_session.query(ESP32Device) \
            .filter(ESP32Device.device_id.__eq__(device_id)) \
            .all()

    def get_by_bucket_id(self, bucket_id: int):
        return self.db_session.query(ESP32Device) \
            .filter(ESP32Device.bucket_id.__eq__(bucket_id)) \
            .all()

    def update_status(self, esp_id: str, status: str):
        try:
            esp_device = self.get_by_esp_id(esp_id)
            if esp_device:
                esp_device.status = status
                esp_device.last_seen = datetime.now(UTC)
                self.db_session.commit()
            return esp_device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def update_heartbeat(self, esp_id: str, status: str = 'active'):
        try:
            esp_device = self.get_by_esp_id(esp_id)
            if esp_device:
                esp_device.status = status
                esp_device.last_seen = datetime.now(UTC)
                self.db_session.commit()
            return esp_device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def update_metadata(self, esp_id: str, device_metadata: dict):
        try:
            esp_device = self.get_by_esp_id(esp_id)
            if esp_device:
                esp_device.device_metadata = device_metadata
                self.db_session.commit()
            return esp_device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def assign_to_bucket(self, esp_id: str, bucket_id: int):
        try:
            esp_device = self.get_by_esp_id(esp_id)
            if esp_device:
                esp_device.bucket_id = bucket_id
                self.db_session.commit()
            return esp_device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def sync_esp32_configuration(self, device_id: int, esp_id: str, name: str = None,
                                 location: str = None, capabilities: list = None,
                                 device_metadata: dict = None, user_id: int = None) -> ESP32Device:
        try:
            esp_device = self.get_by_esp_id(esp_id)
            if esp_device:
                if name:
                    esp_device.name = name
                if location:
                    esp_device.location = location
                if capabilities:
                    esp_device.capabilities = capabilities
                if device_metadata:
                    esp_device.device_metadata = device_metadata
                self.db_session.commit()
            else:
                if user_id is None:
                    # Get the user_id from the associated device
                    device = self.db_session.query(Device).filter(Device.id.__eq__(device_id)).first()
                    if not device:
                        raise SQLAlchemyError(f"Device with id {device_id} not found")
                    user_id = device.user_id

                esp_device = self.create_esp32(
                    device_id=device_id,
                    esp_id=esp_id,
                    name=name or f"ESP32_{esp_id}",
                    location=location or "Unknown",
                    capabilities=capabilities or [],
                    device_metadata=device_metadata or {},
                    user_id=user_id
                )
            return esp_device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def delete_by_esp_id(self, esp_id: str) -> bool:
        try:
            esp_device = self.get_by_esp_id(esp_id)
            if esp_device:
                self.db_session.delete(esp_device)
                self.db_session.commit()
                return True
            return False
        except Exception as e:
            self.db_session.rollback()
            raise e

    def get_active_esp32s(self):
        return self.db_session.query(ESP32Device) \
            .filter(ESP32Device.status.__eq__('active')) \
            .all()

    def get_esp32s_by_capability(self, capability: str):
        return self.db_session.query(ESP32Device) \
            .filter(ESP32Device.capabilities.contains([capability])) \
            .all()

    def update(self, esp_device: ESP32Device):
        try:
            existing_esp = self.get_by_esp_id(esp_device.esp_id)
            if existing_esp is None:
                raise SQLAlchemyError(f"ESP32 device with id {esp_device.esp_id} not found")

            for key, value in esp_device.__dict__.items():
                if not key.startswith('_') and value is not None:
                    setattr(existing_esp, key, value)

            self.db_session.commit()
            return existing_esp
        except SQLAlchemyError:
            self.db_session.rollback()
            raise
