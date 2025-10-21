from datetime import datetime
from typing import Literal

from pybet import Odds
from pydantic import BaseModel, ConfigDict, Field

from .py_object_id import PyObjectId


class MongoBetfairHorseraceBetHistory(BaseModel):
    id: PyObjectId | None = Field(alias="_id", default=None)
    racecourse: str
    race_datetime: datetime
    race_description: str | None = None
    horse: str
    bet_type: Literal["BACK", "LAY"]
    places: int | None = None
    odds: Odds
    stake: float
    profit_loss: float

    model_config = ConfigDict(arbitrary_types_allowed=True)
