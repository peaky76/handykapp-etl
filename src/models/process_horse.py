from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from .mongo_horse import MongoOfficialRatings, MongoOperation
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
    operations: Optional[List[MongoOperation]] = None
    ratings: Optional[MongoOfficialRatings] = None
    race_id: PyObjectId