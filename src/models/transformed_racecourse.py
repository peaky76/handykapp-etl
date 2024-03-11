from typing import Literal, Optional

from .process_base_model import ProcessBaseModel

CodeType = Literal["Flat", "NH"]
SurfaceType = Literal["Turf", "Dirt", "Fibresand", "Polytrack", "Tapeta"]
ObstacleType = Literal["Hurdle", "Chase"]
ShapeType = Literal["Circle", "Horseshoe", "Oval", "Pear", "Triangle"]
Handedness = Literal["Left", "Right", "Both"]
StyleType = Literal["Galloping", "Stiff", "Tight"]
ContourType = Literal["Flat", "Undulating", "Uphill"]


class TransformedRacecourse(ProcessBaseModel):
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
