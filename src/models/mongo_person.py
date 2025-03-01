from typing import Optional

from pydantic import BaseModel, Field

from .mongo_references import MongoReferences
from .py_object_id import PyObjectId


class MongoPerson(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    title: Optional[str] = None
    first: Optional[str] = None
    middle: Optional[str] = None
    last: str
    suffix: Optional[str] = None
    nickname: Optional[str] = None
    references: Optional[MongoReferences] = None
