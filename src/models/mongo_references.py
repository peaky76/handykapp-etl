from typing import Optional

from pydantic import BaseModel


class MongoReferences(BaseModel):
    bha: Optional[str] = None
    racing_research: Optional[str] = None
