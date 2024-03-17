from .formdata_entry import FormdataEntry
from .mongo_base_model import MongoBaseModel


class MongoFormdataEntry(MongoBaseModel, FormdataEntry):
    pass