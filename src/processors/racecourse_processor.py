from functools import cache

from clients import mongo_client as client
from models import TransformedRacecourse

from .processor import Processor
from .utils import compact


class RacecourseProcessor(Processor):
    _descriptor = "racecourse"
    _next_processor = None
    _table = client.handykapp.racecourses

    @cache
    def _search_dictionary(self, racecourse: TransformedRacecourse) -> dict:
        return compact(racecourse.model_dump(include=["name", "country", "obstacle", "surface"]))

    @cache
    def _update_dictionary(self, racecourse: TransformedRacecourse) -> dict:  
        return compact({"references": { f"{racecourse.source}": racecourse.abbr } } |  racecourse.model_dump(exclude=["abbr", "source"]))

    @cache
    def _insert_dictionary(self, racecourse: TransformedRacecourse) -> dict:
        return compact({"references": { f"{racecourse.source}": racecourse.abbr } }  |  racecourse.model_dump(exclude=["abbr", "source"]))

racecourse_processor = RacecourseProcessor().process
