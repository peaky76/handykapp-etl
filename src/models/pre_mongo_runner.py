from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, Field
from pydantic_extra_types.pendulum_dt import Duration


class PreMongoRunner(BaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: Optional[str] = Field(None, min_length=2, max_length=3)
    year: Optional[int] = None
    sex: Optional[Literal["M", "F"]] = None
    colour: Optional[str] = None
    owner: Optional[str] = None
    lbs_carried: Optional[int] = None
    allowance: Optional[int] = None
    saddlecloth: Optional[int] = None
    draw: Optional[int] = None
    headgear: Optional[str] = None
    official_rating: Optional[int] = None
    trainer: Optional[str] = None
    jockey: Optional[str] = None
    sire: Optional[str] = None
    dam: Optional[str] = None
    damsire: Optional[str] = None
    finishing_position: Optional[str] = None
    official_position: Optional[str] = None
    beaten_distance: Optional[float] = None
    time: Optional[Duration] = None
    sp: Optional[Decimal] = None

    model_config = {"frozen": True}

    def __hash__(self):
        return hash(
            (
                self.name,
                self.country,
                self.year,
            )
        )
