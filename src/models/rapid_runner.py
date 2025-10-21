from pydantic import BaseModel, Field


class RapidRunner(BaseModel):
    horse: str = Field(..., description="Name of the horse")
    id_horse: str = Field(..., description="ID of the horse")
    jockey: str = Field(..., description="Name of the jockey")
    trainer: str = Field(..., description="Name of the trainer")
    age: int = Field(..., description="Age of the horse")
    weight: str = Field(..., description="Weight carried by the horse")
    number: int | None = Field(..., description="Saddlecloth number")
    last_ran_days_ago: int | None = Field(
        None, description="Days since the horse last ran"
    )
    non_runner: bool = Field(..., description="Indicates if the horse is a non-runner")
    form: str | None = Field(None, description="Recent form of the horse")
    position: str | None = Field(None, description="Finishing position")
    distance_beaten: str | None = Field(None, description="Distance beaten")
    owner: str | None = Field(None, description="Owner of the horse")
    sire: str | None = Field(None, description="Sire of the horse")
    dam: str | None = Field(None, description="Dam of the horse")
    OR: str | None = Field(None, description="Official rating")
    sp: str | None = Field(None, description="Starting price")
    odds: list = Field(default_factory=list, description="Odds for the horse")
