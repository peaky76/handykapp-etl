from typing import Literal, Optional

from pydantic import BaseModel, Field


class ProcessHorseCore(BaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Literal["M", "F"]] = None

    def __hash__(self):
        return hash(tuple(self.__dict__.values()))