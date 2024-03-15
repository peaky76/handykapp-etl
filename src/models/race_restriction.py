from typing import Optional

from .hashable_base_model import HashableBaseModel


class RaceRestriction(HashableBaseModel):
    minimum: Optional[int] = None
    maximum: Optional[int] = None