from typing import List, Literal, Optional

from pydantic import Field

from .official_ratings import OfficialRatings
from .operation import Operation
from .process_base_model import ProcessBaseModel
from .process_horse_core import ProcessHorseCore
from .py_object_id import PyObjectId


class ProcessHorse(ProcessBaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Literal["M", "F"]] = None
    year: int
    breed: Optional[str] = None
    colour: Optional[str] = None
    sire: Optional[ProcessHorseCore] = None
    dam: Optional[ProcessHorseCore] = None
    jockey: Optional[str] = None
    trainer: Optional[str] = None
    operations: Optional[List[Operation]] = None
    ratings: Optional[OfficialRatings] = None
    race_id: Optional[PyObjectId] = None
    source: Literal["bha", "rapid", "theracingapi"]