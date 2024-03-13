from typing import Annotated, List

from pydantic import Field, StringConstraints

from .transformed_base_model import TransformedBaseModel
from .transformed_formdata_run import TransformedFormdataRun


class TransformedFormdataEntry(TransformedBaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    year: int
    trainer: str
    trainer_form: Annotated[str, StringConstraints(pattern=r'^F[1-5]|F-$')]
    prize_money: Annotated[str, StringConstraints(pattern=r'^£[1-9][0-9]*|£-$')]
    runs: List[TransformedFormdataRun]