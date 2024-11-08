from sqlalchemy import Column, BigInteger, Sequence, VARCHAR, TIMESTAMP
from sqlalchemy.orm import relationship
from datetime import datetime
from .base import Base


class User(Base):
    __tablename__ = 'user'

    id = Column(BigInteger, Sequence('user_id_seq'), primary_key=True)
    cognito_sub = Column(VARCHAR(255), unique=True, nullable=False)
    email = Column(VARCHAR(255), unique=True, nullable=False)
    name = Column(VARCHAR(255))
    family_name = Column(VARCHAR(20))
    created_at = Column(TIMESTAMP)

    def to_dict(self):
        return {
            'id': self.id,
            'cognito_sub': self.cognito_sub,
            'email': self.email,
            'name': self.name,
            'created_at': self.created_at.strftime('%Y-%m-%d %H:%M:%S') if self.created_at else None
            #'roles': [role.role.name for role in self.roles] if self.roles else []
        }
