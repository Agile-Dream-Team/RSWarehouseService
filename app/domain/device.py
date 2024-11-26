# app/domain/device.py
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, JSON, BigInteger
from sqlalchemy.orm import relationship
from app.config.database import Base
from datetime import datetime, UTC


class Device(Base):
    __tablename__ = 'device'  # Changed from 'devices' to 'device' to match ESP32Device reference

    id = Column(BigInteger, primary_key=True)
    name = Column(String)
    serial = Column(String, unique=True)
    device_type = Column(String)
    status = Column(String)
    ip_address = Column(String)
    port = Column(Integer)
    location = Column(String)
    last_seen = Column(DateTime, default=datetime.now(UTC))
    device_metadata = Column(JSON, default={})
    pod_id = Column(BigInteger, ForeignKey('pod.id'))
    user_id = Column(BigInteger)

    # Relationships
    pod = relationship("Pod", back_populates="devices")
    esp_devices = relationship("ESP32Device", back_populates="device", cascade="all, delete-orphan")
    buckets = relationship("Bucket", back_populates="device")

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "serial": self.serial,
            "device_type": self.device_type,
            "status": self.status,
            "ip_address": self.ip_address,
            "port": self.port,
            "location": self.location,
            "last_seen": self.last_seen.isoformat() if self.last_seen else None,
            "device_metadata": self.device_metadata,
            "pod_id": self.pod_id,
            "user_id": self.user_id,
            "esp_devices": [esp.to_dict() for esp in self.esp_devices] if self.esp_devices else []
        }
