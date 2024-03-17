from functools import cache

from models import MongoRacecourse, Racecourse

from .database_processor import DatabaseProcessor
from .utils import compact


class RacecourseProcessor(DatabaseProcessor[Racecourse, MongoRacecourse]):
    _db_model = MongoRacecourse

    @cache
    def _search_dictionary(self, racecourse: Racecourse) -> dict:
        return compact({ 
            "name": racecourse.name.title(), 
            "country": racecourse.country, 
            "obstacle": racecourse.obstacle, 
            "surface": {"$in": ["Tapeta", "Polytrack"]} if racecourse.surface == "AW" else racecourse.surface 
         })

    @cache
    def _update_dictionary(self, racecourse: Racecourse) -> dict:  
        return self._insert_dictionary(racecourse)

    @cache
    def _insert_dictionary(self, racecourse: Racecourse) -> dict:
        return compact({"references": { f"{racecourse.source}": racecourse.abbr } }  |  racecourse.model_dump(exclude={"abbr", "source"}))
