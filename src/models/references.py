from typing import Optional

from .hashable_base_model import HashableBaseModel


class References(HashableBaseModel):
    bha: Optional[str] = None
    racing_research: Optional[str] = None
