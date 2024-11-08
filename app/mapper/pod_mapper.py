import json
from app.domain.pod import Pod


def dto_to_entity(pod_dto: dict):
    try:
        pod_args = {}

        # Add fields that are present in the input
        for field in ['id', 'name', 'description', 'user_id']:
            if field in pod_dto:
                pod_args[field] = pod_dto[field]

        return Pod(**pod_args)
    except json.JSONDecodeError as e:
        print(f"JSONDecodeError: {e}")
        return None
    except Exception as e:
        print(f"Error in pod_dto_to_entity: {e}")
        return None

