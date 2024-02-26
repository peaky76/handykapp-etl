from typing import Optional

from pydantic import BaseModel, Field

from models.mongo_references import MongoReferences

from .pyobjectid import PyObjectId


class MongoRacecourse(BaseModel):
    id: Optional[PyObjectId] = Field(alias="_id", default=None)
    name: str
    formal_name: Optional[str] = None
    surface: Optional[str] = None
    obstacle: str | None
    shape: Optional[str] = None
    handedness: Optional[str] = None
    style: Optional[str] = None
    contour: Optional[str] = None
    references: Optional[MongoReferences] = None
