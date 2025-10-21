from datetime import datetime

from pydantic import BaseModel, Field

from .py_object_id import PyObjectId


class MongoBetfairHorseracePnl(BaseModel):
    id: PyObjectId | None = Field(alias="_id", default=None)
    racecourse: str
    race_datetime: datetime
    race_description: str | None = None
    profit_loss: float
    places: int | None = None
