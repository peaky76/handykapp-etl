from datetime import date
from typing import Optional

from .transformed_base_model import TransformedBaseModel


class Operation(TransformedBaseModel):
    operation_type: str
    date: Optional[date] = None
