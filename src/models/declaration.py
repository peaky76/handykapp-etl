from typing import Optional

from pendulum import datetime

from .hashable_base_model import HashableBaseModel
from .race_restriction import RaceRestriction
from .racecourse_fields import CodeType
from .runner import Runner
from .source import Source


class Declaration(HashableBaseModel):
    racecourse: str
    surface: str
    datetime: datetime
    title: str
    is_handicap: Optional[bool] = None
    code: Optional[CodeType] = None
    obstacle: Optional[str] = None
    distance_description: str
    race_grade: Optional[str] = None
    race_class: Optional[int] = None
    age_restriction: Optional[RaceRestriction] = None
    rating_restriction: Optional[RaceRestriction] = None
    prize: Optional[str] = None
    going_description: Optional[str] = None
    number_of_runners: int
    runners: list[Runner]
    rapid_id: Optional[str] = None
    source: Source
