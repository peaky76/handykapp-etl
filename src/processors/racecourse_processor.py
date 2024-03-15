from functools import cache
from typing import ClassVar

from models import Racecourse

from .database_processor import DatabaseProcessor
from .utils import compact


class RacecourseProcessor(DatabaseProcessor):
    _search_keys: ClassVar[str] = ["name", "country", "obstacle", "surface"]

    @cache
    def _update_dictionary(self, racecourse: Racecourse) -> dict:  
        return self._insert_dictionary(racecourse)

    @cache
    def _insert_dictionary(self, racecourse: Racecourse) -> dict:
        return compact({"references": { f"{racecourse.source}": racecourse.abbr } }  |  racecourse.model_dump(exclude=["abbr", "source"]))
