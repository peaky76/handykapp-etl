from typing import Annotated

from pydantic import Field

Year = Annotated[int, Field(gte=1600, lte=2100)]