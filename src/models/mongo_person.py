from typing import TypedDict
from models.mongo_references import MongoReferences


class MongoPerson(TypedDict, total=False):
    title: str
    first: str
    middle: str
    last: str
    suffix: str
    nickname: str
    references: MongoReferences
