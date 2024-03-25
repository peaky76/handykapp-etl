from typing import Optional

from pydantic import Field

from .hashable_base_model import HashableBaseModel
from .sex import Sex
from .source import Source


class HorseCore(HashableBaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    sex: Optional[Sex] = None
    year: Optional[int] = None
    source: Source