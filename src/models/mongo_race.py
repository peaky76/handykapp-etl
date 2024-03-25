from datetime import datetime
from typing import Optional

from .mongo_base_model import MongoBaseModel
from .mongo_run import MongoRun
from .py_object_id import PyObjectId
from .race_restriction import RaceRestriction
from .references import References
from .source import Source


class MongoRace(MongoBaseModel):
    racecourse: PyObjectId
    datetime: datetime
    title: str
    is_handicap: Optional[bool] = None
    distance_description: str
    race_grade: Optional[str] = None
    race_class: Optional[int] = None
    age_restriction: Optional[RaceRestriction] = None
    rating_restriction: Optional[RaceRestriction] = None
    prize: Optional[str] = None
    going_description: Optional[str] = None
    number_of_runners: Optional[int] = None
    runners: list[MongoRun]
    references: Optional[References] = None
    source: Source