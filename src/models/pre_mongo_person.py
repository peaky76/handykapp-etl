from typing import Annotated, Literal, Optional

from pydantic import BaseModel, Field

from models.py_object_id import PyObjectId


class PreMongoPerson(BaseModel):
    name: str = Field(..., min_length=3)
    role: Literal["jockey", "owner", "trainer"]
    race_id: Optional[PyObjectId] = None
    runner_id: Optional[PyObjectId] = None
    ratings: dict[Annotated[str, Field(min_length=4, max_length=4)], str] = {}

    model_config = {"frozen": True}
