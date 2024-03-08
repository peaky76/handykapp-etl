from .mongo_horse import MongoHorse
from .py_object_id import PyObjectId


class ProcessHorse(MongoHorse):
    race_id: PyObjectId