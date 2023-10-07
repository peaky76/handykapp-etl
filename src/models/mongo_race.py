from bson import ObjectId
from pendulum import DateTime
from typing import TypedDict


class MongoRace(TypedDict, total=False):
    racecourse: ObjectId
    datetime: DateTime
