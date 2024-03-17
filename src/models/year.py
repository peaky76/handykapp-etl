from pydantic import Field

Year = Field(..., ge=1500, le=2100)