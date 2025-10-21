from pydantic import BaseModel


class MongoReferences(BaseModel):
    bha: str | None = None
    racing_research: str | None = None
