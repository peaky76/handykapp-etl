import datetime

from pydantic import BaseModel, Field

from models.bha_shared_types import BirthYear, HorseName, PerfFig, Sex


class BHARatingsRecord(BaseModel):
    date: datetime.date = Field(..., description="Date and time of the rating")
    name: HorseName = Field(..., description="Name of the horse")
    year: BirthYear = Field(..., description="Birth year of the horse")
    sex: Sex = Field(..., description="Sex/gender of the horse")
    trainer: str = Field(..., description="Name of the trainer")
    perf_figs: list[PerfFig] = Field(
        ...,
        min_length=6,
        max_length=6,
        description="List of ratings over the last 6 runs",
    )
