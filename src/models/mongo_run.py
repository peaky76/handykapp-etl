from typing import Optional

from pydantic import BaseModel

from .py_object_id import PyObjectId


class MongoRun(BaseModel):
    horse: PyObjectId
    jockey: Optional[PyObjectId] = None
    trainer: Optional[PyObjectId] = None
    owner: Optional[str] = None
    allowance: Optional[int] = None
    saddlecloth: Optional[int] = None
    draw: Optional[int] = None
    headgear: Optional[str] = None
    lbs_carried: Optional[int] = None 
    official_rating: Optional[int] = None
    position: Optional[int | str] = None
    beaten_distance: Optional[int | str] = None
    sp: Optional[str] = None