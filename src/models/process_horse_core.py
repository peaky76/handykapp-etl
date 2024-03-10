from typing import Literal, Optional

from pydantic import Field

from .process_base_model import ProcessBaseModel


class ProcessHorseCore(ProcessBaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Literal["M", "F"]] = None