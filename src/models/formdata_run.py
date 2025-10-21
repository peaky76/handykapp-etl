from pydantic import BaseModel, Field

from .formdata_position import FormdataPosition


class FormdataRun(BaseModel):
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    race_type: str
    win_prize: str
    course: str = Field(..., min_length=3, max_length=3)
    number_of_runners: int
    weight: str = Field(..., pattern=r"^\d{1,2}-\d{1,2}$")
    headgear: str | None = None
    allowance: int | None = None
    jockey: str
    position: FormdataPosition
    beaten_distance: float | None = None
    time_rating: int | None = None
    distance: float
    going: str
    form_rating: int | None = None
