from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload
from app.domain.device import Device


class DeviceRepository:
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def save(self, device: Device) -> None:
        try:
            self.db_session.add(device)
            self.db_session.commit()
        except Exception as e:
            self.db_session.rollback()
            raise e

    def get_all(self):
        return self.db_session.query(Device).all()

    def get_by_id(self, device_id: int):
        return self.db_session.query(Device).filter(Device.id.__eq__(device_id)).first()

    def get_by_pod_id(self, pod_id: int):
        """
        Retrieve all devices associated with a specific pod
        """
        return self.db_session.query(Device).filter(Device.pod_id.__eq__(pod_id)).all()

    def get_by_user_id(self, user_id: int):
        """
        Retrieve all devices associated with a specific user
        """
        return self.db_session.query(Device).filter(Device.user_id.__eq__(user_id)).all()

    def get_by_id_with_buckets(self, device_id: int):
        """
        Retrieve a device with its associated buckets
        """
        return self.db_session.query(Device) \
            .filter(Device.id.__eq__(device_id)) \
            .options(joinedload(Device.buckets)) \
            .first()

    def delete(self, device: Device) -> None:
        try:
            self.db_session.delete(device)
            self.db_session.commit()
        except Exception as e:
            self.db_session.rollback()
            raise e

    def update(self, device: Device):
        try:
            existing_pod = self.db_session.query(Device).filter_by(id=device.id).first()
            if existing_pod is None:
                raise SQLAlchemyError(f"Device with id {device.id} not found")

            # Update only the attributes that were provided
            for key, value in device.__dict__.items():
                if not key.startswith('_') and value is not None:
                    setattr(existing_pod, key, value)

            self.db_session.commit()
        except SQLAlchemyError:
            self.db_session.rollback()
            raise
