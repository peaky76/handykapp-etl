from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from .official_ratings import OfficialRatings
from .operation import Operation
from .py_object_id import PyObjectId
from .source import Source


class MongoHorse(BaseModel):
    _id: PyObjectId
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Literal["M", "F"]] = None
    year: int
    breed: Optional[str] = None
    colour: Optional[str] = None
    sire: Optional[PyObjectId] = None
    dam: Optional[PyObjectId] = None
    operations: Optional[List[Operation]] = None
    ratings: Optional[OfficialRatings] = None
    trainer: Optional[PyObjectId] = None
    source: Source