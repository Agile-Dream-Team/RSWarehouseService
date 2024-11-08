import json
from app.domain.device import Device


def dto_to_entity(device_dto: dict):
    try:
        device_args = {}

        # Add fields that are present in the input
        for field in ['id', 'pod_id', 'serial', 'name', 'description', 'user_id']:
            if field in device_dto:
                device_args[field] = device_dto[field]

        return Device(**device_args)
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e}")
        return None
    except Exception as e:
        print(f"Error in pod_dto_to_entity: {e}")
        return None
