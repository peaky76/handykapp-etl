from models import HorseCore, MongoHorse

from .database_processor import DatabaseProcessor


class HorseCoreProcessor(DatabaseProcessor[HorseCore, MongoHorse]):
    _descriptor = 'horse'
    _table_name = "horses"
    _db_model = MongoHorse
    