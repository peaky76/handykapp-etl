from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from .official_ratings import OfficialRatings
from .operation import Operation
from .py_object_id import PyObjectId


class ProcessHorse(BaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    year: int
    sex: Optional[Literal["M", "F"]] = None
    breed: Optional[str] = None
    colour: Optional[str] = None
    sire: Optional[str] = None
    dam: Optional[str] = None
    jockey: Optional[str] = None
    trainer: Optional[str] = None
    operations: Optional[List[Operation]] = None
    ratings: Optional[OfficialRatings] = None
    race_id: Optional[PyObjectId] = None