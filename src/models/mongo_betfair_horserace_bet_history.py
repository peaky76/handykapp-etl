from datetime import datetime
from typing import Literal, Optional

from pybet import Odds
from pydantic import BaseModel, Field

from .py_object_id import PyObjectId


class MongoBetfairHorseracePnl(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    racecourse: str
    race_datetime: datetime
    race_description: Optional[str] = None
    horse: str
    bet_type: Literal['BACK', 'LAY']
    odds: Odds
    stake: float
    profit_loss: float
