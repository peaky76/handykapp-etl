from typing import Literal, Optional

from pydantic import Field

import models.racecourse_fields as rf

from .transformed_base_model import TransformedBaseModel


class TransformedRacecourse(TransformedBaseModel):
    name: str
    formal_name: Optional[str] = None
    abbr: Optional[str] = None
    country: str = Field(..., min_length=2, max_length=3)
    code: Optional[rf.CodeType] = None
    surface: Optional[rf.SurfaceType] = None
    obstacle: rf.ObstacleType | None
    shape: Optional[rf.ShapeType] = None
    handedness: Optional[rf.Handedness] = None
    style: Optional[rf.StyleType] = None
    contour: Optional[rf.ContourType] = None
    source: Literal["bha", "rapid", "racing_research", "theracingapi"]
