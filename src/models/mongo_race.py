from bson import ObjectId
from pendulum import DateTime
from typing import TypedDict


class MongoRaceRestriction(TypedDict):
    minimum: int | None
    maximum: int | None


class MongoDeclaration(TypedDict, total=False):
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
    prize: str


class MongoRace(MongoDeclaration, total=False):
    going_description: str
