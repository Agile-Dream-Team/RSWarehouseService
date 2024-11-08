from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.domain.bucket import Bucket


class BucketRepository:
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def save(self, bucket: Bucket) -> None:
        try:
            self.db_session.add(bucket)
            self.db_session.commit()
        except Exception as e:
            self.db_session.rollback()
            raise e

    def get_all(self):
        return self.db_session.query(Bucket).all()

    def get_by_id(self, bucket_id: int):
        return self.db_session.query(Bucket).filter(Bucket.id.__eq__(bucket_id)).first()

    def get_buckets_by_device_id(self, device_id: int):
        """
        Retrieve all buckets associated with a specific device
        """
        return self.db_session.query(Bucket).filter(Bucket.device_id.__eq__(device_id)).all()

    def delete(self, bucket: Bucket) -> None:
        try:
            self.db_session.delete(bucket)
            self.db_session.commit()
        except Exception as e:
            self.db_session.rollback()
            raise e

    def update(self, bucket: Bucket):
        try:
            existing_pod = self.db_session.query(Bucket).filter_by(id=bucket.id).first()
            if existing_pod is None:
                raise SQLAlchemyError(f"Bucket with id {bucket.id} not found")

            # Update only the attributes that were provided
            for key, value in bucket.__dict__.items():
                if not key.startswith('_') and value is not None:
                    setattr(existing_pod, key, value)

            self.db_session.commit()
        except SQLAlchemyError:
            self.db_session.rollback()
            raise
