from typing import Optional

from .transformed_base_model import TransformedBaseModel


class OfficialRatings(TransformedBaseModel):
    flat: Optional[int] = None
    aw: Optional[int] = None
    chase: Optional[int] = None
    hurdle: Optional[int] = None