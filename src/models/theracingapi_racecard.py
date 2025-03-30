from pydantic import BaseModel, Field

from .theracingapi_runner import TheRacingApiRunner  # Assuming runners are of this type


class TheRacingApiRacecard(BaseModel):
    course: str = Field(..., description="Name of the racecourse")
    date: str = Field(..., description="Date of the race in YYYY-MM-DD format")
    off_time: str = Field(..., description="Scheduled off time of the race")
    race_name: str = Field(..., description="Name of the race")
    distance_f: float = Field(..., description="Distance of the race in furlongs")
    region: str = Field(..., description="Region of the race")
    pattern: str = Field(..., description="Pattern of the race (e.g., Group 1, Listed)")
    race_class: str = Field(..., description="Class of the race")
    race_type: str = Field(
        ..., alias="type", description="Type of the race (e.g., Hurdle, Flat)"
    )
    age_band: str = Field(..., description="Age band restriction for the race")
    rating_band: str = Field(..., description="Rating band restriction for the race")
    prize: str = Field(..., description="Prize money for the race")
    field_size: int = Field(..., description="Number of runners in the race")
    going: str = Field(..., description="Going description for the race")
    surface: str = Field(..., description="Surface type (e.g., Turf, AW)")
    runners: list[TheRacingApiRunner] = Field(
        default_factory=list, description="List of runners in the race"
    )
