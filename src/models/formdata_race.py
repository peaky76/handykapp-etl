from pydantic import BaseModel, Field


class FormdataRace(BaseModel):
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    race_type: str
    win_prize: str
    course: str = Field(..., min_length=3, max_length=3)
    number_of_runners: int
    distance: float
    going: str

    model_config = {"frozen": True}

    def __hash__(self):
        return hash(
            (
                self.date,
                self.race_type,
                self.win_prize,
                self.course,
                self.number_of_runners,
                self.distance,
                self.going,
            )
        )
