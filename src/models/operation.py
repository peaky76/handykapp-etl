from datetime import date
from typing import Optional

from .hashable_base_model import HashableBaseModel


class Operation(HashableBaseModel):
    operation_type: str
    date: Optional[date] = None
