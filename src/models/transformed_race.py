from typing import Literal

from .mongo_race import MongoRace
from .transformed_base_model import TransformedBaseModel
from .transformed_horse import TransformedHorse


class TransformedRace(MongoRace, TransformedBaseModel):
    runners: list[TransformedHorse]
    source: Literal["bha", "rapid", "racing_research", "theracingapi"]