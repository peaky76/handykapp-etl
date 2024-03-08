from typing import Optional

from pydantic import BaseModel


class OfficialRatings(BaseModel):
    flat: Optional[int] = None
    aw: Optional[int] = None
    chase: Optional[int] = None
    hurdle: Optional[int] = None