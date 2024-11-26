import json
import logging
from app.domain.device import Device
from datetime import datetime, UTC


def dto_to_entity(device_dto: dict) -> Device | None:
    """
    Convert device DTO to Device entity

    Args:
        device_dto (dict): Device data transfer object

    Returns:
        Device: Device entity if successful, None otherwise
    """
    try:
        device_args = {
            'name': device_dto.get('name'),
            'serial': device_dto.get('serial'),
            'device_type': device_dto.get('device_type', 'raspberry_pi'),
            'status': device_dto.get('status', 'active'),
            'ip_address': device_dto.get('ip_address'),
            'port': device_dto.get('port'),
            'location': device_dto.get('location'),
            'last_seen': device_dto.get('last_seen', datetime.now(UTC)),
            'device_metadata': device_dto.get('device_metadata', {}),
            'pod_id': device_dto.get('pod_id'),
            'user_id': device_dto.get('user_id')
        }

        # Remove None values
        device_args = {k: v for k, v in device_args.items() if v is not None}

        # Create and return the Device entity
        return Device(**device_args)

    except json.JSONDecodeError as e:
        logging.error(f"JSON Decode Error in dto_to_entity: {str(e)}")
        return None
    except Exception as e:
        logging.error(f"Error in dto_to_entity: {str(e)}")
        return None