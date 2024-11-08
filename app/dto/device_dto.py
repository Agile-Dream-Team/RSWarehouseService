from pydantic import BaseModel, Field
from typing import Optional


class DeviceDTO(BaseModel):
    id: Optional[int] = None
    pod_id: int = Field(..., gt=0)
    serial: str = Field(..., min_length=1, max_length=255)
    name: str = Field(..., min_length=1, max_length=255)
    description: str = Field(..., max_length=255)
    user_id: Optional[int] = None

    class Config:
        from_attributes = True
