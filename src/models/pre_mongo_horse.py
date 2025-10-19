import datetime
from typing import Literal

from pydantic import BaseModel, Field

from models.mongo_horse import MongoOfficialRatings


class PreMongoHorse(BaseModel):
    name: str = Field(..., min_length=2, max_length=21)
    country: str | None = Field(default=None, min_length=2, max_length=3)
    year: int | None = None
    sex: Literal["M", "F"] | None = None
    gelded_from: datetime.date | None = None
    colour: str | None = None
    owner: str | None = None
    trainer: str | None = None
    sire: str | None = None
    dam: str | None = None
    damsire: str | None = None
    ratings: MongoOfficialRatings | None = None

    model_config = {"frozen": True}

    def __hash__(self):
        return hash(
            (
                self.name,
                self.country,
                self.year,
            )
        )
