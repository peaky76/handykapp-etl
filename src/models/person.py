from typing import Literal, Optional

from .hashable_base_model import HashableBaseModel
from .references import References


class Person(HashableBaseModel):
    name: str
    role: Literal['jockey', 'owner', 'trainer']
    references: Optional[References] = None
    