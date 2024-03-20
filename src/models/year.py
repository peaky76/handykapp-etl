from typing import Annotated

from pydantic import Field, StringConstraints

Year = Annotated[str, Field(StringConstraints(pattern=r'^[12]\d{3}$'))]