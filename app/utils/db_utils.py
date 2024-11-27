import logging
from contextlib import contextmanager
from sqlalchemy.exc import SQLAlchemyError
from RSErrorHandler.ErrorHandler import RSKafkaException


@contextmanager
def transaction_scope(db_session, kafka_client, kafka_topic):
    """Provide a transactional scope around a series of operations."""
    try:
        yield
        db_session.commit()
    except SQLAlchemyError as e:
        db_session.rollback()
        error_msg = f"Database error: {str(e)}"
        logging.error(error_msg)
        raise RSKafkaException(error_msg, kafka_client, kafka_topic)
    except Exception as e:
        db_session.rollback()
        error_msg = f"Unexpected error: {str(e)}"
        logging.error(error_msg)
        raise RSKafkaException(error_msg, kafka_client, kafka_topic)
    finally:
        db_session.close()
