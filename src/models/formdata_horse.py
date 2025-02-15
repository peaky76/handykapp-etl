from pydantic import BaseModel, Field


class FormdataHorse(BaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    year: int
    trainer: str
    trainer_form: str = Field(..., pattern=r"^F[1-5-]$")
    prize_money: str = Field(..., pattern=r"^£(\d+|-)$")
    runs: list = []
