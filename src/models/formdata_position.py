import re
from typing import Annotated

from pydantic import BeforeValidator


def validate_position(v: str) -> str:
    if not isinstance(v, str):
        raise ValueError("Position must be a string")

    position_patterns = (
        r"^\d+$"  # Simple digit (e.g., "1", "2", "10")
        r"|^=\d+$"  # Equals sign followed by digit (e.g., "=1", "=2")
        r"|^\d+p\d+$"  # Digit followed by 'p' and another digit (e.g., "1p2", "3p4")
        r"|^[a-z]$"  # Any single lowercase letter (e.g., "p", "u", "f")
    )

    if re.match(position_patterns, v):
        return v

    raise ValueError(f"Invalid position format: {v}.")


FormdataPosition = Annotated[str, BeforeValidator(validate_position)]
