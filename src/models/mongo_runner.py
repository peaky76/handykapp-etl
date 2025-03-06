from typing import Optional

from pydantic import BaseModel
from pydantic_extra_types.pendulum_dt import Duration

from .py_object_id import PyObjectId


class MongoRunner(BaseModel):
    horse: PyObjectId
    owner: Optional[str] = None
    lbs_carried: Optional[int] = None
    allowance: Optional[int] = None
    saddlecloth: Optional[int] = None
    draw: Optional[int] = None
    headgear: Optional[str] = None
    official_rating: Optional[int] = None
    trainer: Optional[PyObjectId] = None
    jockey: Optional[PyObjectId] = None
    sire: Optional[PyObjectId] = None
    dam: Optional[PyObjectId] = None
    damsire: Optional[PyObjectId] = None
    position: Optional[str] = None
    beaten_distance: Optional[float] = None
    time: Optional[Duration] = None
