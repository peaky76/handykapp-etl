from models import MongoRacecourse, Racecourse

from .database_processor import DatabaseProcessor
from .utils import compact


class RacecourseProcessor(DatabaseProcessor[Racecourse, MongoRacecourse]):
    _db_model = MongoRacecourse

    def find(self, racecourse: MongoRacecourse) -> MongoRacecourse | None:
        search_dictionary = compact({ 
            "name": racecourse.name.title(), 
            "country": racecourse.country, 
            "obstacle": racecourse.obstacle, 
            "surface": {"$in": ["Tapeta", "Polytrack"]} if racecourse.surface == "AW" else racecourse.surface 
         })
        db_item = self._table.find_one(search_dictionary)
        return MongoRacecourse(**dict(db_item | {"db_id": db_item["_id"]})) if db_item else None
