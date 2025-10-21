from typing import Literal

from pydantic import BaseModel, Field

from models.mongo_references import MongoReferences
from models.py_object_id import PyObjectId

CodeType = Literal["Flat", "NH"]
SurfaceType = Literal["Turf", "Dirt", "Fibresand", "Polytrack", "Tapeta"]
ObstacleType = Literal["Hurdle", "Chase"]
ShapeType = Literal["Circle", "Horseshoe", "Oval", "Pear", "Triangle"]
Handedness = Literal["Left", "Right", "Both"]
StyleType = Literal["Galloping", "Stiff", "Tight"]
ContourType = Literal["Flat", "Undulating", "Uphill"]


class MongoRacecourse(BaseModel):
    id: PyObjectId | None = Field(alias="_id", default=None)
    name: str
    formal_name: str | None = None
    code: CodeType | None = None
    surface: SurfaceType | None = None
    obstacle: ObstacleType | None
    shape: ShapeType | None = None
    handedness: Handedness | None = None
    style: StyleType | None = None
    contour: ContourType | None = None
    references: MongoReferences | None = None
