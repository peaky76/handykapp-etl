import re
from typing import Annotated, Literal, Optional

from pydantic import AfterValidator, BaseModel, BeforeValidator, Field


def validate_horse(v: str) -> str:
    if not v:
        raise ValueError("Horse name cannot be empty")

    has_country = bool(re.search(r"\([A-Z]{2,3}\)", v))
    if not has_country or len(v) > 30:
        raise ValueError(f"Invalid horse name: {v}")
    return v


def validate_year(v: int) -> int:
    if not (1600 <= v <= 2100):
        raise ValueError(f"Invalid year: {v}")
    return v


def empty_string_to_none(v):
    if isinstance(v, str) and v.strip() == "":
        return None
    return v


def validate_rating(v: Optional[int]) -> Optional[int]:
    if v is None:
        return v
    if not (0 <= v <= 240):
        raise ValueError(f"Invalid rating: {v}")
    return v


HorseName = Annotated[str, AfterValidator(validate_horse)]
BirthYear = Annotated[int, AfterValidator(validate_year)]
Rating = Annotated[
    Optional[int],
    BeforeValidator(empty_string_to_none),
    AfterValidator(validate_rating),
]


class BHARatingsRecord(BaseModel):
    name: HorseName = Field(..., description="Name of the horse")
    year: BirthYear = Field(..., description="Birth year of the horse")
    sex: Literal["GELDING", "FILLY", "COLT"] = Field(
        ..., description="Sex/gender of the horse"
    )
    sire: HorseName = Field(..., description="Name of the sire")
    dam: HorseName = Field(..., description="Name of the dam")
    trainer: str = Field(..., description="Name of the trainer")
    flat_rating: Rating = Field(None, description="Flat rating")
    diff_flat: Optional[str] = Field(None, description="Difference in flat rating")
    flat_clltrl: Optional[str] = Field(None, description="Flat collateral info")
    awt_rating: Rating = Field(None, description="All-weather track rating")
    diff_awt: Optional[str] = Field(None, description="Difference in AWT rating")
    awt_clltrl: Optional[str] = Field(None, description="AWT collateral info")
    chase_rating: Rating = Field(None, description="Chase rating")
    diff_chase: Optional[str] = Field(None, description="Difference in chase rating")
    chase_clltrl: Optional[str] = Field(None, description="Chase collateral info")
    hurdle_rating: Rating = Field(None, description="Hurdle rating")
    diff_hurdle: Optional[str] = Field(None, description="Difference in hurdle rating")
    hurdle_clltrl: Optional[str] = Field(None, description="Hurdle collateral info")
