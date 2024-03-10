from datetime import date
from typing import Optional

from .process_base_model import ProcessBaseModel


class Operation(ProcessBaseModel):
    operation_type: str
    date: Optional[date] = None
