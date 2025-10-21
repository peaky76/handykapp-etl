from pydantic import BaseModel, Field

from .mongo_references import MongoReferences
from .py_object_id import PyObjectId


class MongoPerson(BaseModel):
    id: PyObjectId | None = Field(alias="_id", default=None)
    title: str | None = None
    first: str | None = None
    middle: str | None = None
    last: str
    suffix: str | None = None
    nickname: str | None = None
    references: MongoReferences | None = None
