from .mongo_base_model import MongoBaseModel
from .racecourse import Racecourse


class MongoRacecourse(MongoBaseModel, Racecourse):
    pass
