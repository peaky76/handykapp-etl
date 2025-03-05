from typing import Optional

from pydantic import BaseModel, Field
from pydantic_extra_types.pendulum_dt import Duration


class PreMongoRunner(BaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    year: int
    owner: Optional[str] = None
    lbs_carried: Optional[int] = None
    allowance: Optional[int] = None
    saddlecloth: Optional[int] = None
    draw: Optional[int] = None
    headgear: Optional[str] = None
    official_rating: Optional[int] = None
    trainer: Optional[str] = None
    jockey: Optional[str] = None
    position: Optional[str] = None
    beaten_distance: Optional[float] = None
    time: Optional[Duration] = None
