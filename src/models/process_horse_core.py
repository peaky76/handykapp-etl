from typing import Literal, Optional

from pydantic import Field

from .hashable_base_model import HashableBaseModel


class ProcessHorseCore(HashableBaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Literal["M", "F"]] = None