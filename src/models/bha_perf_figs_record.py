import datetime

from pydantic import BaseModel, Field

from models.bha_shared_types import BirthYear, HorseName, PerfFig, Sex


class BHAPerfFigsRecord(BaseModel):
    date: datetime.date = Field(..., description="Date of the rating")
    racehorse: HorseName = Field(..., description="Name of the horse")
    yof: BirthYear = Field(..., description="Birth year of the horse")
    sex: Sex = Field(..., description="Sex/gender of the horse")
    trainer: str = Field(..., description="Name of the trainer")
    latest: PerfFig = Field(..., description="Latest performance figure")
    two_runs_ago: PerfFig = Field(..., description="Performance figure 2 runs ago")
    three_runs_ago: PerfFig = Field(..., description="Performance figure 3 runs ago")
    four_runs_ago: PerfFig = Field(..., description="Performance figure 4 runs ago")
    five_runs_ago: PerfFig = Field(..., description="Performance figure 5 runs ago")
    six_runs_ago: PerfFig = Field(..., description="Performance figure 6 runs ago")
