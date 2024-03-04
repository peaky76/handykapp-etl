from datetime import date
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from .py_object_id import PyObjectId


class MongoOperation(BaseModel):
    operation_type: str
    date: Optional[date] = None


class MongoOfficialRatings(BaseModel):
    flat: Optional[int] = None
    aw: Optional[int] = None
    chase: Optional[int] = None
    hurdle: Optional[int] = None


class MongoHorse(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    year: int
    sex: Optional[Literal["M", "F"]] = None
    breed: Optional[str] = None
    colour: Optional[str] = None
    sire: Optional[PyObjectId] = None
    dam: Optional[PyObjectId] = None
    trainer: Optional[PyObjectId] = None
    operations: Optional[List[MongoOperation]] = None
    ratings: Optional[MongoOfficialRatings] = None
