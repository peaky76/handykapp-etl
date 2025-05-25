from typing import Optional

from pydantic import BaseModel, Field

from .formdata_position import FormdataPosition


class FormdataRunner(BaseModel):
    name: str = Field(..., min_length=3, max_length=21)
    country: str = Field(..., min_length=2, max_length=3)
    year: int
    weight: str = Field(..., pattern=r"^\d{1,2}-\d{1,2}$")
    headgear: Optional[str] = None
    allowance: Optional[int] = None
    jockey: str
    position: FormdataPosition
    beaten_distance: Optional[float] = None
    time_rating: Optional[int] = None
    form_rating: Optional[int] = None

    model_config = {"frozen": True}

    def __hash__(self):
        return hash(
            (
                self.name,
                self.country,
                self.year,
            )
        )
