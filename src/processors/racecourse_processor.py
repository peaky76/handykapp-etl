from functools import cache
from typing import ClassVar

from clients import mongo_client as client
from models import TransformedRacecourse
from pymongo.collection import Collection

from .database_processor import DatabaseProcessor
from .utils import compact


class RacecourseProcessor(DatabaseProcessor):
    _descriptor: ClassVar[str] = "racecourse"
    _table: ClassVar[Collection] = client.handykapp.racecourses
    _search_keys: ClassVar[str] = ["name", "country", "obstacle", "surface"]

    @cache
    def _update_dictionary(self, racecourse: TransformedRacecourse) -> dict:  
        return self._insert_dictionary(racecourse)

    @cache
    def _insert_dictionary(self, racecourse: TransformedRacecourse) -> dict:
        return compact({"references": { f"{racecourse.source}": racecourse.abbr } }  |  racecourse.model_dump(exclude=["abbr", "source"]))

racecourse_processor = RacecourseProcessor().process
