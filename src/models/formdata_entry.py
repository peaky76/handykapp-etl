from typing import Annotated, List

from pydantic import Field, StringConstraints

from .formdata_run import FormdataRun
from .hashable_base_model import HashableBaseModel


class FormdataEntry(HashableBaseModel):
    name: str = Field(..., min_length=2, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    year: int
    trainer: str
    trainer_form: Annotated[str, StringConstraints(pattern=r'^F[1-5]|F-$')]
    prize_money: Annotated[str, StringConstraints(pattern=r'^£[1-9][0-9]*|£-$')]
    runs: List[FormdataRun]
    source: str = "racing_research"
