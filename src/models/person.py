from typing import Literal, Optional

from .hashable_base_model import HashableBaseModel
from .references import References
from .sex import Sex


class Person(HashableBaseModel):
    name: str
    sex: Optional[Sex] = None
    role: Literal['jockey', 'owner', 'trainer']
    references: Optional[References] = None
    source: str
    