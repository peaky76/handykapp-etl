from typing import Literal

from pydantic import BaseModel

from .py_object_id import PyObjectId


class ProcessPerson(BaseModel):
    name: str
    role: Literal['jockey', 'owner', 'trainer']
    race_id: PyObjectId
    horse_id: PyObjectId