from typing import TypedDict

from models.mongo_references import MongoReferences


class MongoRacecourse(TypedDict, total=False):
    name: str
    formal_name: str
    surface: str
    obstacle: str | None
    shape: str
    handedness: str
    style: str
    contour: str
    references: MongoReferences
