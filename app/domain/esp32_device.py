from sqlalchemy import Column, BigInteger, VARCHAR, ForeignKey, Sequence, JSON, DateTime
from sqlalchemy.orm import relationship
from app.domain.base import Base
from app.domain.user import User
from datetime import datetime, UTC


class ESP32Device(Base):
    __tablename__ = 'esp32_device'

    id = Column(BigInteger, Sequence('esp32_device_id_seq'), primary_key=True)
    device_id = Column(BigInteger, ForeignKey('device.id'), nullable=False)  # This now correctly references the device table
    bucket_id = Column(BigInteger, ForeignKey('bucket.id'))
    esp_id = Column(VARCHAR(255), unique=True, nullable=False)
    name = Column(VARCHAR(255), nullable=False)
    location = Column(VARCHAR(255))
    status = Column(VARCHAR(50), default='inactive')
    last_seen = Column(DateTime, default=datetime.now(UTC))
    device_metadata = Column(JSON, default={})
    capabilities = Column(JSON, default=[])
    user_id = Column(BigInteger, ForeignKey('user.id'))

    # Relationships
    device = relationship("Device", back_populates="esp_devices")
    bucket = relationship("Bucket", back_populates="esp_devices")
    user = relationship(User)

    def to_dict(self):
        return {
            key: value
            for key, value in {
                'id': self.id,
                'device_id': self.device_id,
                'bucket_id': self.bucket_id,
                'esp_id': self.esp_id,
                'name': self.name,
                'location': self.location,
                'status': self.status,
                'last_seen': self.last_seen.isoformat() if self.last_seen else None,
                'capabilities': self.capabilities,
                'device_metadata': self.device_metadata,
                'user_id': self.user_id
            }.items()
            if value is not None
        }
