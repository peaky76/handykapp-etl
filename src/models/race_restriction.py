from typing import Optional

from .transformed_base_model import TransformedBaseModel


class RaceRestriction(TransformedBaseModel):
    minimum: Optional[int] = None
    maximum: Optional[int] = None