from .mongo_base_model import MongoBaseModel
from .person import Person


class MongoPerson(MongoBaseModel, Person):
    pass