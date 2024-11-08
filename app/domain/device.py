from sqlalchemy import Column, BigInteger, VARCHAR, ForeignKey, Sequence
from sqlalchemy.orm import relationship
from app.domain.base import Base
from app.domain.user import User
from app.domain.pod import Pod


class Device(Base):
    __tablename__ = 'device'

    id = Column(BigInteger, Sequence('device_id_seq'), primary_key=True)
    pod_id = Column(BigInteger, ForeignKey('pod.id'))
    serial = Column(VARCHAR(255), unique=True, nullable=False)
    name = Column(VARCHAR(255), nullable=False)
    description = Column(VARCHAR(255))
    user_id = Column(BigInteger, ForeignKey('user.id'))

    # Relationships
    pod = relationship(Pod, back_populates="devices")
    user = relationship(User)
    buckets = relationship("Bucket", back_populates="device", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            key: value
            for key, value in {
                'id': self.id,
                'pod_id': self.pod_id,
                'serial': self.serial,
                'name': self.name,
                'description': self.description,
                'user_id': self.user_id
            }.items()
            if value is not None
        }
