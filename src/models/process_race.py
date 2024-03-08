from .mongo_race import MongoRace
from .process_horse import ProcessHorse


class ProcessRace(MongoRace):
    runners: list[ProcessHorse]