from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from .pyobjectid import PyObjectId


class MongoRaceRestriction(BaseModel):
    minimum: Optional[int] = None
    maximum: Optional[int] = None


class MongoRace(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    racecourse: PyObjectId
    datetime: datetime
    title: str
    is_handicap: Optional[bool] = None
    obstacle: Optional[str] = None
    distance_description: str
    race_grade: Optional[str] = None
    race_class: Optional[int] = None
    age_restriction: Optional[MongoRaceRestriction] = None
    rating_restriction: Optional[MongoRaceRestriction] = None
    prize: Optional[str] = None
    going_description: Optional[str] = None
