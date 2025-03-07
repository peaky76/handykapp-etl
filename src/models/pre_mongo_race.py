from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from .pre_mongo_runner import PreMongoRunner


class PreMongoRace(BaseModel):
    rapid_id: Optional[str] = None
    course: str
    obstacle: str
    surface: str
    code: str
    datetime: datetime
    title: str
    is_handicap: Optional[bool] = None
    distance_description: str
    race_grade: Optional[str] = None
    race_class: Optional[int] = None
    age_restriction: Optional[str] = None
    rating_restriction: Optional[str] = None
    prize: Optional[str] = None
    going_description: Optional[str] = None
    runners: list[PreMongoRunner] = []

    model_config = {"frozen": True}

    def __hash__(self):
        return hash(
            (
                self.course,
                self.obstacle,
                self.surface,
                self.code,
                self.datetime,
            )
        )
