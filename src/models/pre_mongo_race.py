from datetime import datetime

from pydantic import BaseModel

from .pre_mongo_runner import PreMongoRunner


class PreMongoRace(BaseModel):
    rapid_id: str | None = None
    course: str
    obstacle: str | None = None
    surface: str | None = None
    code: str
    datetime: datetime
    title: str
    is_handicap: bool | None = None
    distance_description: str
    race_grade: str | None = None
    race_class: int | None = None
    age_restriction: str | None = None
    rating_restriction: str | None = None
    prize: str | None = None
    going_description: str | None = None
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
