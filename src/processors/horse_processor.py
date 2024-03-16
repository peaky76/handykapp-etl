from functools import cache
from typing import ClassVar, List

from models import PyObjectId, Horse, HorseCore, Person

from processors.person_processor import PersonProcessor

from .database_processor import DatabaseProcessor
from .processor import Processor
from .utils import compact

InputType = Horse | HorseCore

class HorseProcessor(DatabaseProcessor):
    _next_processors: ClassVar[List[Processor]] = [PersonProcessor]
    _search_keys: ClassVar[List[str]] = ["name", "country", "sex", "year"]

    @cache
    def _update_dictionary(self, horse: Horse | HorseCore) -> dict:
        if isinstance(horse, HorseCore):
            return {}

        return compact({
            "colour": horse.colour,
            "sire": self.find(horse.sire) if horse.sire else None,
            "dam": self.find(horse.dam) if horse.dam else None
        })

    @cache
    def _insert_dictionary(self, horse: InputType) -> dict:
        return compact(self._search_dictionary(horse) | self._update_dictionary(horse))
            
    def post_process(self, horse: InputType, db_id: PyObjectId):
        p = self.running_processors[0]
        
        if isinstance(horse, Horse) and horse.current_trainer:    
            name = horse.current_trainer
            source = horse.source
            person = Person(**{"name": name, "role": "trainer", "source": horse.source})
            p.send(person)

        # if isinstance(horse, Runner) and horse.race_id:
        #     client.handykapp.races.update_one(
        #         {"_id": horse.race_id},
        #         {
        #             "$push": {
        #                 "runners": {"horse": horse.db_id } | 
        #                     compact(horse.model_dump(include=[
        #                         "owner",
        #                         "allowance",
        #                         "saddlecloth",
        #                         "draw",
        #                         "headgear",
        #                         "lbs_carried",
        #                         "official_rating",
        #                         "position",
        #                         "distance_beaten",
        #                         "sp",
        #                 ]))
        #             }
        #         },
        #     )

        #     if horse.jockey:
        #         PersonProcessor.send((
        #             {
        #                 "name": horse["jockey"],
        #                 "role": "jockey",
        #                 "race_id": horse.race_id,
        #                 "horse_id": db_id,
        #             },
        #             horse["source"],
        #             # {},
        #         ))
