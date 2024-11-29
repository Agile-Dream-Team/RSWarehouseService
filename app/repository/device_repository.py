# app/repository/device_repository.py
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import or_
from datetime import datetime, timedelta, UTC
from app.domain.device import Device


class DeviceRepository:
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def save(self, device: Device) -> Device:
        try:
            self.db_session.add(device)
            self.db_session.commit()
            return device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def get_all(self):
        return self.db_session.query(Device) \
            .options(joinedload(Device.esp_devices)) \
            .all()

    def get_by_id(self, device_id: int):
        return self.db_session.query(Device) \
            .filter(Device.id.__eq__(device_id)) \
            .first()

    def get_by_id_with_esp32s(self, device_id: int):
        return self.db_session.query(Device) \
            .filter(Device.id.__eq__(device_id)) \
            .options(joinedload(Device.esp_devices)) \
            .first()

    def get_by_serial_with_esp32s(self, serial: str):
        return self.db_session.query(Device) \
            .filter(Device.serial.__eq__(serial)) \
            .options(joinedload(Device.esp_devices)) \
            .first()

    def get_by_pod_id(self, pod_id: int):
        return self.db_session.query(Device) \
            .filter(Device.pod_id.__eq__(pod_id)) \
            .options(joinedload(Device.esp_devices)) \
            .all()

    def get_by_user_id(self, user_id: int):
        return self.db_session.query(Device) \
            .filter(Device.user_id.__eq__(user_id)) \
            .options(joinedload(Device.esp_devices)) \
            .all()

    def get_by_id_with_buckets(self, device_id: int):
        return self.db_session.query(Device) \
            .filter(Device.id.__eq__(device_id)) \
            .options(joinedload(Device.buckets)) \
            .options(joinedload(Device.esp_devices)) \
            .first()

    def get_active_devices(self):
        return self.db_session.query(Device) \
            .filter(Device.status.__eq__('active')) \
            .options(joinedload(Device.esp_devices)) \
            .all()

    def get_inactive_devices(self, threshold_minutes: int = 5):
        threshold_time = datetime.now(UTC) - timedelta(minutes=threshold_minutes)
        return self.db_session.query(Device)\
            .filter(or_(
                Device.status.__eq__('inactive'),
                Device.last_seen < threshold_time
            ))\
            .options(joinedload(Device.esp_devices))\
            .all()

    def update_status(self, device_id: int, status: str):
        try:
            device = self.get_by_id(device_id)
            if not device:
                raise SQLAlchemyError(f"Device with id {device_id} not found")

            device.status = status
            device.last_seen = datetime.now(UTC)
            self.db_session.commit()
            return device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def update_heartbeat(self, device_id: int, status: str = 'active'):
        try:
            device = self.get_by_id(device_id)
            if not device:
                raise SQLAlchemyError(f"Device with id {device_id} not found")

            device.status = status
            device.last_seen = datetime.now(UTC)
            self.db_session.commit()
            return device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def update_metadata(self, device_id: int, device_metadata: dict):
        try:
            device = self.get_by_id(device_id)
            if device:
                device.device_metadata = device_metadata
                self.db_session.commit()
            return device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def update_device_configuration(self, device_id: int, ip_address: str = None,
                                    port: int = None, name: str = None,
                                    location: str = None, device_metadata: dict = None):
        try:
            device = self.get_by_id(device_id)
            if device:
                if ip_address:
                    device.ip_address = ip_address
                if port:
                    device.port = port
                if name:
                    device.name = name
                if location:
                    device.location = location
                if device_metadata:
                    device.device_metadata = device_metadata
                self.db_session.commit()
            return device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def migrate_device(self, device_id: int, new_pod_id: int):
        try:
            device = self.get_by_id_with_esp32s(device_id)
            if device:
                device.pod_id = new_pod_id
                self.db_session.commit()
            return device
        except Exception as e:
            self.db_session.rollback()
            raise e

    def get_device_statistics(self, device_id: int) -> dict:
        try:
            device = self.get_by_id_with_esp32s(device_id)
            if not device:
                return {}

            return {
                "device_id": device.id,
                "name": device.name,
                "status": device.status,
                "last_seen": device.last_seen,
                "esp32_count": len(device.esp_devices),
                "active_esp32s": sum(1 for esp in device.esp_devices if esp.status == 'active'),
                "esp32_by_status": {
                    status: sum(1 for esp in device.esp_devices if esp.status == status)
                    for status in set(esp.status for esp in device.esp_devices)
                }
            }
        except Exception as e:
            raise e

    def delete(self, device: Device) -> None:
        try:
            self.db_session.delete(device)
            self.db_session.commit()
        except Exception as e:
            self.db_session.rollback()
            raise e

    def update(self, device: Device):
        try:
            existing_device = self.db_session.query(Device).filter_by(id=device.id).first()
            if existing_device is None:
                raise SQLAlchemyError(f"Device with id {device.id} not found")

            for key, value in device.__dict__.items():
                if not key.startswith('_') and value is not None:
                    setattr(existing_device, key, value)

            self.db_session.commit()
            return existing_device
        except SQLAlchemyError:
            self.db_session.rollback()
            raise

    # Add these methods to your DeviceRepository class

    def get_device_summary(self, device_id: int) -> dict:
        try:
            device = self.get_by_id_with_esp32s(device_id)
            if not device:
                return {}

            return {
                "device_id": device.id,
                "name": device.name,
                "status": device.status,
                "last_seen": device.last_seen,
                "total_esp32s": len(device.esp_devices),
                "active_esp32s": sum(1 for esp in device.esp_devices if esp.status == 'active'),
                "inactive_esp32s": sum(1 for esp in device.esp_devices if esp.status == 'inactive'),
                "buckets_count": len(device.buckets) if hasattr(device, 'buckets') else 0,
                "location": device.location,
                "device_type": device.device_type,
                "device_metadata": device.device_metadata
            }
        except Exception as e:
            raise e

    def get_all_devices_summary(self) -> list:
        try:
            devices = self.get_all()
            return [
                {
                    "device_id": device.id,
                    "name": device.name,
                    "status": device.status,
                    "last_seen": device.last_seen,
                    "total_esp32s": len(device.esp_devices),
                    "active_esp32s": sum(1 for esp in device.esp_devices if esp.status == 'active'),
                    "inactive_esp32s": sum(1 for esp in device.esp_devices if esp.status == 'inactive'),
                    "buckets_count": len(device.buckets) if hasattr(device, 'buckets') else 0,
                    "location": device.location,
                    "device_type": device.device_type,
                    "device_metadata": device.device_metadata
                }
                for device in devices
            ]
        except Exception as e:
            raise e

    def get_all_devices_statistics(self) -> dict:
        try:
            devices = self.get_all()
            total_devices = len(devices)
            active_devices = sum(1 for device in devices if device.status == 'active')
            total_esp32s = sum(len(device.esp_devices) for device in devices)
            active_esp32s = sum(
                sum(1 for esp in device.esp_devices if esp.status == 'active')
                for device in devices
            )

            return {
                "total_devices": total_devices,
                "active_devices": active_devices,
                "inactive_devices": total_devices - active_devices,
                "total_esp32s": total_esp32s,
                "active_esp32s": active_esp32s,
                "inactive_esp32s": total_esp32s - active_esp32s,
                "devices_by_status": {
                    status: sum(1 for device in devices if device.status == status)
                    for status in set(device.status for device in devices)
                },
                "esp32s_by_status": {
                    status: sum(
                        sum(1 for esp in device.esp_devices if esp.status == status)
                        for device in devices
                    )
                    for status in set(
                        esp.status
                        for device in devices
                        for esp in device.esp_devices
                    )
                }
            }
        except Exception as e:
            raise e
