from typing import Literal, Optional

from pydantic import BaseModel, Field

from models.mongo_references import MongoReferences

from .py_object_id import PyObjectId

CodeType = Literal["Flat", "NH"]
SurfaceType = Literal["Turf", "Dirt", "Fibresand", "Polytrack", "Tapeta", "Sand", "Snow"]
ObstacleType = Literal["Hurdle", "Chase"]
ShapeType = Literal["Circle", "Horseshoe", "Oval", "Pear", "Triangle"]
Handedness = Literal["Left", "Right", "Both", "Neither"]
StyleType = Literal["Galloping", "Sharp", "Stiff", "Tight"]
ContourType = Literal["Flat", "Undulating", "Uphill"]


class MongoRacecourse(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    name: str
    formal_name: Optional[str] = None
    code: Optional[CodeType] = None
    surface: Optional[SurfaceType] = None
    obstacle: ObstacleType | None
    shape: Optional[ShapeType] = None
    handedness: Optional[Handedness] = None
    style: Optional[StyleType] = None
    contour: Optional[ContourType] = None
    references: Optional[MongoReferences] = None
