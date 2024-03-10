from .mongo_race import MongoRace
from .process_base_model import ProcessBaseModel
from .process_horse import ProcessHorse


class ProcessRace(MongoRace, ProcessBaseModel):
    runners: list[ProcessHorse]