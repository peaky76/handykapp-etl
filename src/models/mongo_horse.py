from datetime import date
from typing import TypedDict

from bson import ObjectId


class MongoOperation(TypedDict):
    operation_type: str
    date: date | None


class MongoOfficialRatings(TypedDict, total=False):
    flat: int | None
    aw: int | None
    chase: int | None
    hurdle: int | None


class MongoHorseBase(TypedDict):
    name: str
    country: str
    year: int
    sex: str


class MongoHorse(MongoHorseBase, total=False):
    sire: ObjectId
    dam: ObjectId
    trainer: ObjectId
    operations: list[MongoOperation]
    ratings: MongoOfficialRatings
