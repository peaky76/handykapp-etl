from datetime import datetime
from typing import Optional

from .py_object_id import PyObjectId
from .race_restriction import RaceRestriction
from .source import Source
from .transformed_base_model import TransformedBaseModel
from .transformed_horse import TransformedHorse


class TransformedRace(TransformedBaseModel):
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
    runners: list[TransformedHorse]
    source: Source