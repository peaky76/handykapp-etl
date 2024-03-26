from typing import Optional

from pydantic import Field

from .mongo_base_model import MongoBaseModel
from .racecourse_fields import (
    CodeType,
    ContourType,
    Handedness,
    ObstacleType,
    ShapeType,
    StyleType,
    SurfaceType,
)


class MongoRacecourse(MongoBaseModel):
    name: str
    formal_name: Optional[str] = None
    country: str = Field(..., min_length=2, max_length=3)
    code: Optional[CodeType] = None
    surface: Optional[SurfaceType] = None
    obstacle: Optional[ObstacleType] = None
    shape: Optional[ShapeType] = None
    handedness: Optional[Handedness] = None
    style: Optional[StyleType] = None
    contour: Optional[ContourType] = None
