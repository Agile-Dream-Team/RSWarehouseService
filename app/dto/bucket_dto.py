from pydantic import BaseModel, Field
from typing import Optional


class BucketDTO(BaseModel):
    id: Optional[int] = None
    serial: str = Field(..., min_length=1, max_length=255)
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=255)
    device_id: int = Field(..., gt=0)
    user_id: Optional[int] = None

    class Config:
        from_attributes = True
