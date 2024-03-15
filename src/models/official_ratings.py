from typing import Optional

from .hashable_base_model import HashableBaseModel


class OfficialRatings(HashableBaseModel):
    flat: Optional[int] = None
    aw: Optional[int] = None
    chase: Optional[int] = None
    hurdle: Optional[int] = None