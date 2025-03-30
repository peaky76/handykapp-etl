from typing import Optional

from pydantic import BaseModel, Field

from .rapid_runner import RapidRunner


class RapidRecord(BaseModel):
    id_race: str = Field(..., description="ID of the race")
    course: str = Field(..., description="Name of the racecourse")
    date: str = Field(..., description="Date and time of the race")
    title: str = Field(..., description="Title of the race")
    distance: str = Field(..., description="Distance of the race")
    age: str = Field(..., description="Age restriction for the race")
    going: str = Field(..., description="Going description for the race")
    finished: bool = Field(..., description="Indicates if the race is finished")
    canceled: bool = Field(..., description="Indicates if the race is canceled")
    finish_time: Optional[str] = Field(None, description="Finish time of the race")
    prize: Optional[str] = Field(None, description="Prize money for the race")
    race_class: Optional[str] = Field(
        None, alias="class", description="Class of the race"
    )
    horses: list[RapidRunner] = Field(..., description="List of runners in the race")
