from sqlalchemy import Column, BigInteger, VARCHAR, ForeignKey, Sequence
from sqlalchemy.orm import relationship
from app.domain.base import Base
from app.domain.user import User


class Pod(Base):
    __tablename__ = 'pod'

    id = Column(BigInteger, Sequence('pod_id_seq'), primary_key=True)
    name = Column(VARCHAR(255), nullable=False)
    description = Column(VARCHAR(255))
    user_id = Column(BigInteger, ForeignKey('user.id'))

    # Relationships
    user = relationship(User)
    devices = relationship("Device", back_populates="pod", cascade="all, delete-orphan")

    def to_dict(self):
        return {
            key: value
            for key, value in {
                'id': self.id,
                'name': self.name,
                'description': self.description,
                'user_id': self.user_id
            }.items()
            if value is not None
        }
