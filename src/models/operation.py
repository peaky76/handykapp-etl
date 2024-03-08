from datetime import date
from typing import Optional

from pydantic import BaseModel


class Operation(BaseModel):
    operation_type: str
    date: Optional[date] = None