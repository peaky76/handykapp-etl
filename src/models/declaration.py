from datetime import datetime
from typing import Optional

from .hashable_base_model import HashableBaseModel
from .race_restriction import RaceRestriction
from .racecourse import Racecourse
from .references import References
from .runner import Runner
from .source import Source


class Declaration(HashableBaseModel):
    racecourse: Racecourse
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
    number_of_runners: int
    runners: list[Runner]
    references: Optional[References] = None
    source: Source