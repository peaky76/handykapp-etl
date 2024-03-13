from typing import Literal

from .py_object_id import PyObjectId
from .source import Source
from .transformed_base_model import TransformedBaseModel


class TransformedPerson(TransformedBaseModel):
    name: str
    role: Literal['jockey', 'owner', 'trainer']
    race_id: PyObjectId
    horse_id: PyObjectId
    source: Source