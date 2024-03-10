from typing import Literal

from .process_base_model import ProcessBaseModel
from .py_object_id import PyObjectId


class ProcessPerson(ProcessBaseModel):
    name: str
    role: Literal['jockey', 'owner', 'trainer']
    race_id: PyObjectId
    horse_id: PyObjectId
    source: Literal["bha", "rapid", "theracingapi"]