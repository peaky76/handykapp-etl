from .person import Person
from .py_object_id import PyObjectId


class MongoPerson(Person):
    _id: PyObjectId