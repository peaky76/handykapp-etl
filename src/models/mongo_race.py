from bson import ObjectId
from pendulum import DateTime
from typing import TypedDict


class MongoRaceRestriction(TypedDict):
    minimum: int | None
    maximum: int | None


class MongoRace(TypedDict, total=False):
    racecourse: ObjectId
    datetime: DateTime
    title: str
    is_handicap: bool
    obstacle: str | None
    distance_description: str
    race_grade: str
    race_class: int
    age_restriction: MongoRaceRestriction
    rating_restriction: MongoRaceRestriction
    going_description: str
    prize: str
