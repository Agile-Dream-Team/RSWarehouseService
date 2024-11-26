from sqlalchemy import Column, BigInteger, VARCHAR, ForeignKey, Sequence
from sqlalchemy.orm import relationship
from app.domain.base import Base
from app.domain.user import User
from app.domain.device import Device


class Bucket(Base):
    __tablename__ = 'bucket'

    id = Column(BigInteger, Sequence('bucket_id_seq'), primary_key=True)
    serial = Column(VARCHAR(255), unique=True, nullable=False)
    name = Column(VARCHAR(255), nullable=False)
    description = Column(VARCHAR(255))
    device_id = Column(BigInteger, ForeignKey('device.id'))
    user_id = Column(BigInteger, ForeignKey('user.id'))

    # Relationships
    device = relationship(Device, back_populates="buckets")
    user = relationship(User)
    esp_devices = relationship("ESP32Device", back_populates="bucket", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            key: value
            for key, value in {
                'id': self.id,
                'serial': self.serial,
                'name': self.name,
                'description': self.description,
                'device_id': self.device_id,
                'user_id': self.user_id,
                'esp_devices': [esp.to_dict() for esp in self.esp_devices] if self.esp_devices else None
            }.items()
            if value is not None
        }

    def to_dict_without_esp(self):
        """
        Returns dictionary representation without ESP devices to avoid circular references
        """
        return {
            key: value
            for key, value in {
                'id': self.id,
                'serial': self.serial,
                'name': self.name,
                'description': self.description,
                'device_id': self.device_id,
                'user_id': self.user_id
            }.items()
            if value is not None
        }
