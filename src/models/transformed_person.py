from typing import Literal

from .transformed_base_model import TransformedBaseModel
from .py_object_id import PyObjectId


class TransformedPerson(TransformedBaseModel):
    name: str
    role: Literal['jockey', 'owner', 'trainer']
    race_id: PyObjectId
    horse_id: PyObjectId
    source: Literal["bha", "rapid", "theracingapi"]