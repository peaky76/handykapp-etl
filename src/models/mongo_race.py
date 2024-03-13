from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from .py_object_id import PyObjectId
from .race_restriction import RaceRestriction


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
    age_restriction: Optional[RaceRestriction] = None
    rating_restriction: Optional[RaceRestriction] = None
    prize: Optional[str] = None
    going_description: Optional[str] = None
