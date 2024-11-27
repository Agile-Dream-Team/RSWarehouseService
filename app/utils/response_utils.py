from datetime import datetime, UTC
from typing import Dict, Any, Optional
from fastapi import status


def create_response(
        status_code: int = status.HTTP_200_OK,
        message: str = "Success",
        data: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Create a standardized response dictionary with HTTP status codes.

    Args:
        status_code: HTTP status code from fastapi . status
        message: Response message
        data: Optional data payload
    """
    response = {
        "status_code": status_code,
        "message": message,
        "timestamp": datetime.now(UTC).isoformat()
    }
    if data:
        response.update(data)
    return response
