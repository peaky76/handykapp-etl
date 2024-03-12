from datetime import datetime
from typing import Literal

from pydantic import Field, constr

from .transformed_base_model import TransformedBaseModel


class TransformedFormdataRun(TransformedBaseModel):
    date: datetime
    race_type: str
    win_prize: int
    course: str = Field(..., min_length=2, max_length=3)
    number_of_runners: int
    weight: constr(regex='^(7|8|9|10|11|12)-([0-9]|10|11|12|13)$')
    headgear: str
    allowance: int
    jockey: str
    position: str
    beaten_distance: float
    time_rating: int
    distance: float
    going: Literal["H", "F", "M", "G", "D", "S", "V", "f", "m", "g", "d", "s"]
    form_rating: int
