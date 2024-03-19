from typing import ClassVar, List, Set

from bson import ObjectId
from models import Horse, MongoHorse

from processors.horse_core_processor import HorseCoreProcessor
from processors.person_processor import PersonProcessor

from .database_processor import DatabaseProcessor
from .processor import Processor
from .utils import compact


class HorseProcessor(DatabaseProcessor[Horse, MongoHorse]):
    _db_model = MongoHorse
    _forward_processors: ClassVar[List[Processor]] = [HorseCoreProcessor(), PersonProcessor()]
    _search_keys: ClassVar[Set[str]] = {"name", "country", "sex", "year"}

    def __init__(self):
        super().__init__()
        self.sire_ids = {}
        self.dam_ids = {}
        self.trainer_id = None
        # self.person_ids = {}

    def _update_dictionary(self, horse: Horse) -> dict:
        return compact({
            "colour": horse.colour,
            "sire": self.find(horse.sire) if horse.sire else None,
            "dam": self.find(horse.dam) if horse.dam else None
        })

    def pre_process(self, horse: Horse) -> None:
        h = self.running_processors[0]
        p = self.running_processors[1]  

        def sire_callback(x):
            self.sire_ids[horse.sire] = x

        def dam_callback(x):
            self.dam_ids[horse.dam] = x

        def person_callback(x):
            # self.person_ids[horse.trainer] = x
            self.trainer_id = x

        if not (sire_id := self.sire_ids.get(horse.sire)):
            h.send((horse.sire, sire_callback))
            sire_id = self.sire_ids[horse.sire]

        if not (dam_id := self.dam_ids.get(horse.dam)):
            h.send((horse.dam, dam_callback))
            dam_id = self.dam_ids[horse.dam]

        # if not (trainer_id := self.person_ids.get(horse.trainer)):
        p.send((horse.trainer, person_callback))
        trainer_id = self.trainer_id
            # trainer_id = self.person_ids[horse.trainer]

        return MongoHorse(**(horse.model_dump() | {"sire": ObjectId(sire_id), "dam": ObjectId(dam_id), "trainer": ObjectId(trainer_id)}))   