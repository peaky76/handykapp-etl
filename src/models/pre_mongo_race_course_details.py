from pydantic import BaseModel


class PreMongoRaceCourseDetails(BaseModel):
    course: str
    obstacle: str | None = None
    surface: str | None = None
    code: str

    model_config = {"frozen": True}

    def __hash__(self):
        return hash((self.course, self.obstacle, self.surface, self.code))
