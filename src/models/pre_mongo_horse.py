import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field

from models.mongo_horse import MongoOfficialRatings


class PreMongoHorse(BaseModel):
    name: str = Field(..., min_length=2, max_length=21)
    country: Optional[str] = Field(default=None, min_length=2, max_length=3)
    year: Optional[int] = None
    sex: Optional[Literal["M", "F"]] = None
    gelded_from: Optional[datetime.date] = None
    colour: Optional[str] = None
    owner: Optional[str] = None
    trainer: Optional[str] = None
    sire: Optional[str] = None
    dam: Optional[str] = None
    ratings: Optional[MongoOfficialRatings] = None
