from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, joinedload

from app.domain.device import Device
from app.domain.pod import Pod


class PodRepository:
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def save(self, pod: Pod) -> None:
        try:
            self.db_session.add(pod)
            self.db_session.commit()
        except Exception as e:
            self.db_session.rollback()
            raise e

    def get_all(self):
        return self.db_session.query(Pod).all()

    def get_by_id(self, pod_id: int):
        return self.db_session.query(Pod).filter(Pod.id.__eq__(pod_id)).first()

    def get_by_user_id(self, user_id: int):
        """
        Retrieve all pods associated with a specific user
        """
        return self.db_session.query(Pod).filter(Pod.user_id.__eq__(user_id)).all()

    def get_by_id_with_devices(self, pod_id: int):
        """
        Retrieve a pod with its associated devices
        """
        return self.db_session.query(Pod) \
            .filter(Pod.id.__eq__(pod_id)) \
            .options(joinedload(Pod.devices)) \
            .first()

    def get_with_devices_and_buckets(self, pod_id: int):
        """
        Retrieve a pod with its associated devices and their buckets
        """
        return self.db_session.query(Pod) \
            .filter(Pod.id.__eq__(pod_id)) \
            .options(
            joinedload(Pod.devices).joinedload(Device.buckets)
        ) \
            .first()

    def delete(self, pod: Pod) -> None:
        try:
            self.db_session.delete(pod)
            self.db_session.commit()
        except Exception as e:
            self.db_session.rollback()
            raise e

    def update(self, pod: Pod):
        try:
            existing_pod = self.db_session.query(Pod).filter_by(id=pod.id).first()
            if existing_pod is None:
                raise SQLAlchemyError(f"Pod with id {pod.id} not found")

            # Update only the attributes that were provided
            for key, value in pod.__dict__.items():
                if not key.startswith('_') and value is not None:
                    setattr(existing_pod, key, value)

            self.db_session.commit()
        except SQLAlchemyError:
            self.db_session.rollback()
            raise
