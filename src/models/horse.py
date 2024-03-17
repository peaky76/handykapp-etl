from typing import List, Literal, Optional

from pydantic import Field

from .hashable_base_model import HashableBaseModel
from .horse_core import HorseCore
from .official_ratings import OfficialRatings
from .operation import Operation
from .source import Source


class Horse(HashableBaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Literal["M", "F"]] = None
    year: int
    breed: Optional[str] = None
    colour: Optional[str] = None
    sire: Optional[HorseCore] = None
    dam: Optional[HorseCore] = None
    damsire: Optional[HorseCore] = None
    operations: Optional[List[Operation]] = None
    ratings: Optional[OfficialRatings] = None
    trainer: Optional[str] = None
    source: Source