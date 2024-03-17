from .formdata_entry import FormdataEntry
from .py_object_id import PyObjectId


class MongoFormdataEntry(FormdataEntry):
    _id: PyObjectId
