from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from .mongo_race import MongoRaceRestriction
from .pre_mongo_runner import PreMongoRunner


class PreMongoRace(BaseModel):
    course: str
    obstacle: str
    surface: str
    code: str
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
    runners: list[PreMongoRunner] = []
