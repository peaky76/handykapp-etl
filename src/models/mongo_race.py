from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from .mongo_runner import MongoRunner
from .py_object_id import PyObjectId


class MongoRace(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    rapid_id: Optional[str] = None
    racecourse: PyObjectId
    datetime: datetime
    title: str
    is_handicap: Optional[bool] = None
    obstacle: Optional[str] = None
    distance_description: str
    race_grade: Optional[str] = None
    race_class: Optional[int] = None
    age_restriction: Optional[str] = None
    rating_restriction: Optional[str] = None
    prize: Optional[str] = None
    going_description: Optional[str] = None
    runners: list[MongoRunner] = []
