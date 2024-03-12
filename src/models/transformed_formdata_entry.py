from typing import List

from pydantic import Field, constr

from .transformed_base_model import TransformedBaseModel
from .transformed_formdata_run import TransformedFormdataRun


class TransformedFormdataEntry(TransformedBaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    year: int
    trainer: str
    trainer_form: constr(regex='^F[1-5]|F-$')
    prize_money: constr(regex='^£[1-9][0-9]*|£-$')
    runs: List[TransformedFormdataRun]