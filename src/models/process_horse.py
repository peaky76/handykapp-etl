from typing import List, Optional

from .official_ratings import OfficialRatings
from .operation import Operation
from .process_horse_core import ProcessHorseCore
from .py_object_id import PyObjectId


class ProcessHorse(ProcessHorseCore):
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