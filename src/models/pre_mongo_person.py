from typing import Annotated, Literal

from pydantic import BaseModel, Field

from models.py_object_id import PyObjectId

Role = Literal["jockey", "owner", "trainer"]


class PreMongoPerson(BaseModel):
    name: str = Field(..., min_length=3)
    role: Role
    race_id: PyObjectId | None = None
    runner_id: PyObjectId | None = None
    ratings: dict[Annotated[str, Field(min_length=4, max_length=4)], str] = {}

    model_config = {"frozen": True}
