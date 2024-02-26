from typing import Optional

from pydantic import BaseModel

from models.mongo_references import MongoReferences


class MongoPerson(BaseModel):
    title: Optional[str] = None
    first: Optional[str] = None
    middle: Optional[str] = None
    last: str
    suffix: Optional[str] = None
    nickname: Optional[str] = None
    references: Optional[MongoReferences] = None
