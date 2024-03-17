from models import MongoRacecourse, Racecourse

from .database_processor import DatabaseProcessor
from .utils import compact


class RacecourseProcessor(DatabaseProcessor[Racecourse, MongoRacecourse]):
    _db_model = MongoRacecourse
    
    def _search_dictionary(self, racecourse: Racecourse) -> dict:
        return compact({ 
            "name": racecourse.name.title(), 
            "country": racecourse.country, 
            "obstacle": racecourse.obstacle, 
            "surface": {"$in": ["Tapeta", "Polytrack"]} if racecourse.surface == "AW" else racecourse.surface 
         })
