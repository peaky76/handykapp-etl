from typing import Literal

from pydantic import BaseModel
from pydantic_extra_types.pendulum_dt import Date

from models.bha_shared_types import Rating
from models.mongo_racecourse import ObstacleType


class HistoricRatingRecord(BaseModel):
    rating: Rating
    date_before: Date
    races_before: int
    surface: Literal["Turf", "All Weather"]
    obstacle: ObstacleType | None
