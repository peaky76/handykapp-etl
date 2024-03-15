from typing import Literal

from .py_object_id import PyObjectId
from .source import Source
from .hashable_base_model import HashableBaseModel


class Person(HashableBaseModel):
    name: str
    role: Literal['jockey', 'owner', 'trainer']
    race_id: PyObjectId
    horse_id: PyObjectId
    source: Source