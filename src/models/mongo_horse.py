from typing import List, Optional

from pydantic import Field

from .mongo_base_model import MongoBaseModel
from .official_ratings import OfficialRatings
from .operation import Operation
from .py_object_id import PyObjectId
from .references import References
from .sex import Sex
from .source import Source


class MongoHorse(MongoBaseModel):    
    name: str = Field(..., min_length=2, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Sex] = None
    year: Optional[int] = None
    breed: Optional[str] = None
    colour: Optional[str] = None
    sire: Optional[PyObjectId] = None
    dam: Optional[PyObjectId] = None
    operations: Optional[List[Operation]] = None
    ratings: Optional[OfficialRatings] = None
    trainer: Optional[PyObjectId] = None
    references: Optional[References] = None
    source: Source