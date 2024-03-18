from models import HorseCore, MongoHorse

from .database_processor import DatabaseProcessor


class HorseCoreProcessor(DatabaseProcessor[HorseCore, MongoHorse]):
    _descriptor = 'horse pedigree'
    _table_name = "horses"
    _business_model = HorseCore
    _db_model = MongoHorse
