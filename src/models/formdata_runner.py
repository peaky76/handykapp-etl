from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field


class FormdataRunner(BaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    year: int
    weight: str = Field(..., pattern=r"^\d{1,2}-\d{1,2}$")
    headgear: Optional[str] = None
    allowance: Optional[int] = None
    jockey: str
    position: str
    beaten_distance: Optional[Decimal] = None
    time_rating: Optional[int] = None
    form_rating: Optional[int] = None
