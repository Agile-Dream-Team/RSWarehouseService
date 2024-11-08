from pydantic import BaseModel


class PodDTO(BaseModel):
    name: str
    description: str | None = None
    user_id: int | None = None


class UpdatePodDTO(BaseModel):
    id: int
    name: str | None = None
    description: str | None = None
    user_id: int | None = None
