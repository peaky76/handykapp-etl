from typing import Optional

from pydantic import Field

from .hashable_base_model import HashableBaseModel
from .racecourse_fields import (
    CodeType,
    ContourType,
    Handedness,
    ObstacleType,
    ShapeType,
    StyleType,
    SurfaceType,
)
from .references import References
from .source import Source


class Racecourse(HashableBaseModel):
    name: str
    formal_name: Optional[str] = None
    country: str = Field(..., min_length=2, max_length=3)
    code: Optional[CodeType] = None
    surface: Optional[SurfaceType] = None
    obstacle: Optional[ObstacleType] = None
    variant: Optional[str] = None
    shape: Optional[ShapeType] = None
    handedness: Optional[Handedness] = None
    style: Optional[StyleType] = None
    contour: Optional[ContourType] = None
    references: Optional[References] = None
    source: Source
