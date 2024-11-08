import json
from app.domain.bucket import Bucket


def dto_to_entity(device_dto: dict):
    try:
        # Only include fields that are actually present in the input dictionary
        bucket_args = {}

        # Add fields that are present in the input
        for field in ['id', 'serial', 'name', 'description', 'device_id', 'user_id']:
            if field in device_dto:
                bucket_args[field] = device_dto[field]

        return Bucket(**bucket_args)
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e}")
        return None
    except Exception as e:
        print(f"Error in pod_dto_to_entity: {e}")
        return None
