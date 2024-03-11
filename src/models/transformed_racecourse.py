from typing import Literal, Optional

from .transformed_base_model import TransformedBaseModel

CodeType = Literal["Flat", "NH"]
SurfaceType = Literal["Turf", "Dirt", "Fibresand", "Polytrack", "Tapeta", "Sand", "Snow"]
ObstacleType = Literal["Hurdle", "Chase"]
ShapeType = Literal["Circle", "Horseshoe", "Oval", "Pear", "Triangle"]
Handedness = Literal["Left", "Right", "Both", "Neither"]
StyleType = Literal["Galloping", "Sharp", "Stiff", "Tight"]
ContourType = Literal["Flat", "Undulating", "Uphill"]


class TransformedRacecourse(TransformedBaseModel):
    name: str
    formal_name: Optional[str] = None
    abbr: Optional[str] = None
    code: Optional[CodeType] = None
    surface: Optional[SurfaceType] = None
    obstacle: ObstacleType | None
    shape: Optional[ShapeType] = None
    handedness: Optional[Handedness] = None
    style: Optional[StyleType] = None
    contour: Optional[ContourType] = None
    source: Literal["bha", "rapid", "rr", "theracingapi"]
