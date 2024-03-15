from datetime import datetime
from typing import Optional

from .py_object_id import PyObjectId
from .race_restriction import RaceRestriction
from .source import Source
from .hashable_base_model import HashableBaseModel
from .horse import Horse


class Race(HashableBaseModel):
    racecourse_id: PyObjectId
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
    runners: list[Horse]
    rapid_id: Optional[str] = None
    source: Source