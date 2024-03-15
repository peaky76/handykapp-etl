from datetime import datetime
from typing import Annotated, Literal, Optional

from pydantic import Field, StringConstraints

from .hashable_base_model import HashableBaseModel


class FormdataRun(HashableBaseModel):
    date: datetime
    race_type: str
    win_prize: int
    course: str = Field(..., min_length=2, max_length=3)
    number_of_runners: int
    weight: Annotated[str, StringConstraints(pattern=r'^(7|8|9|10|11|12)-([0-9]|10|11|12|13)$')]
    headgear: Optional[str] = None
    allowance: Optional[int] = None
    jockey: str
    position: str
    beaten_distance: float
    time_rating: Optional[int | str]
    distance: float | str
    going: Literal["H", "F", "M", "G", "D", "S", "V", "f", "m", "g", "d", "s"]
    form_rating: Optional[int | str]
