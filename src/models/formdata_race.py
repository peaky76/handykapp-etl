from pydantic import BaseModel, Field


class FormdataRace(BaseModel):
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    race_type: str
    win_prize: str
    course: str = Field(..., min_length=3, max_length=3)
    number_of_runners: int
    distance: float
    going: str
