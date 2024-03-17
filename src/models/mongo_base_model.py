from pydantic import BaseModel

from .py_object_id import PyObjectId


class MongoBaseModel(BaseModel):
    _id: PyObjectId