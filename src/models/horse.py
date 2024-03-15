from typing import List, Literal, Optional

from pydantic import Field

from .official_ratings import OfficialRatings
from .operation import Operation
from .py_object_id import PyObjectId
from .source import Source
from .hashable_base_model import HashableBaseModel
from .horse_core import HorseCore


class Horse(HashableBaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Literal["M", "F"]] = None
    year: int
    breed: Optional[str] = None
    colour: Optional[str] = None
    sire: Optional[HorseCore] = None
    dam: Optional[HorseCore] = None
    operations: Optional[List[Operation]] = None
    ratings: Optional[OfficialRatings] = None
    current_trainer: Optional[str] = None
    source: Source