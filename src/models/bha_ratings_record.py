import datetime

from pydantic import BaseModel, Field

from models.bha_shared_types import BirthYear, HorseName, Rating, Sex


class BHARatingsRecord(BaseModel):
    date: datetime.date = Field(..., description="Date and time of the rating")
    name: HorseName = Field(..., description="Name of the horse")
    year: BirthYear = Field(..., description="Birth year of the horse")
    sex: Sex = Field(..., description="Sex/gender of the horse")
    sire: HorseName = Field(..., description="Name of the sire")
    dam: HorseName = Field(..., description="Name of the dam")
    trainer: str = Field(..., description="Name of the trainer")
    flat_rating: Rating = Field(None, description="Flat rating")
    diff_flat: str | None = Field(None, description="Difference in flat rating")
    flat_clltrl: str | None = Field(None, description="Flat collateral info")
    awt_rating: Rating = Field(None, description="All-weather track rating")
    diff_awt: str | None = Field(None, description="Difference in AWT rating")
    awt_clltrl: str | None = Field(None, description="AWT collateral info")
    chase_rating: Rating = Field(None, description="Chase rating")
    diff_chase: str | None = Field(None, description="Difference in chase rating")
    chase_clltrl: str | None = Field(None, description="Chase collateral info")
    hurdle_rating: Rating = Field(None, description="Hurdle rating")
    diff_hurdle: str | None = Field(None, description="Difference in hurdle rating")
    hurdle_clltrl: str | None = Field(None, description="Hurdle collateral info")
