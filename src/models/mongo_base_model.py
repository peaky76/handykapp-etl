from typing import Optional

from pydantic import BaseModel

from .py_object_id import PyObjectId
from .references import References
from .source import Source


class MongoBaseModel(BaseModel):
    db_id: Optional[PyObjectId] = None
    references: Optional[References] = None
    source: Source
