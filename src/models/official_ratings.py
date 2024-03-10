from typing import Optional

from .process_base_model import ProcessBaseModel


class OfficialRatings(ProcessBaseModel):
    flat: Optional[int] = None
    aw: Optional[int] = None
    chase: Optional[int] = None
    hurdle: Optional[int] = None