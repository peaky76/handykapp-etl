from .py_object_id import PyObjectId
from .racecourse import Racecourse


class MongoRacecourse(Racecourse):
    _id: PyObjectId
