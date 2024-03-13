from typing import Optional

from pydantic import BaseModel, Field

import models.racecourse_fields as rf
from models.mongo_references import MongoReferences

from .py_object_id import PyObjectId


class MongoRacecourse(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    name: str
    formal_name: Optional[str] = None
    code: Optional[rf.CodeType] = None
    surface: Optional[rf.SurfaceType] = None
    obstacle: rf.ObstacleType | None
    shape: Optional[rf.ShapeType] = None
    handedness: Optional[rf.Handedness] = None
    style: Optional[rf.StyleType] = None
    contour: Optional[rf.ContourType] = None
    references: Optional[MongoReferences] = None
