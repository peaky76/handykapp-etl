from typing import List, Literal, Optional

from pydantic import Field

from .official_ratings import OfficialRatings
from .operation import Operation
from .py_object_id import PyObjectId
from .transformed_base_model import TransformedBaseModel
from .transformed_horse_core import TransformedHorseCore


class TransformedHorse(TransformedBaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Literal["M", "F"]] = None
    year: int
    breed: Optional[str] = None
    colour: Optional[str] = None
    sire: Optional[TransformedHorseCore] = None
    dam: Optional[TransformedHorseCore] = None
    operations: Optional[List[Operation]] = None
    ratings: Optional[OfficialRatings] = None
    race_id: Optional[PyObjectId] = None
    source: Literal["bha", "rapid", "racing_research", "theracingapi"]