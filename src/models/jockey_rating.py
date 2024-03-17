from pydantic import Field

JockeyRating = Field(pattern=r"^[0-2]?\d{1}\.\d{1}\*{0,3}$")