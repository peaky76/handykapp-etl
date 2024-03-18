from typing import Optional

from pydantic import BaseModel

from .py_object_id import PyObjectId


class MongoBaseModel(BaseModel):
    db_id: Optional[PyObjectId] = None
