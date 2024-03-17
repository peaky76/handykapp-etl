from typing import Optional

from .mongo_base_model import MongoBaseModel
from .references import References


class MongoPerson(MongoBaseModel):
    title: Optional[str] = None
    first: str
    middle: Optional[str] = None
    last: str
    suffix: Optional[str] = None
    nickname: Optional[str] = None
    role: Optional[str] = None
    references: Optional[References] = None