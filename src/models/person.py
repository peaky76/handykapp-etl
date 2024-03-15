from typing import Literal, Optional

from .hashable_base_model import HashableBaseModel
from .py_object_id import PyObjectId
from .source import Source


class Person(HashableBaseModel):
    name: str
    role: Literal['jockey', 'owner', 'trainer']
    race_id: Optional[PyObjectId] = None
    horse_id: Optional[PyObjectId] = None
    source: Source
    