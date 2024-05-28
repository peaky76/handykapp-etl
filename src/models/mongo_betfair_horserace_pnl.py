from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field

from .py_object_id import PyObjectId


class MongoBetfairHorseracePnl(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    racecourse: str
    race_datetime: datetime
    profit_loss: float
    places: Optional[int] = None
    race_description: Optional[str] = None
    