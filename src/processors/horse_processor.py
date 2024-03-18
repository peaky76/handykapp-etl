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
        self.person_ids = {}

    def _update_dictionary(self, horse: Horse) -> dict:
        return compact({
            "colour": horse.colour,
            "sire": self.find(horse.sire) if horse.sire else None,
            "dam": self.find(horse.dam) if horse.dam else None
        })

    def pre_process(self, horse: Horse) -> None:
        h = self.running_processors[0]

        def sire_callback(x):
            self.sire_ids[horse.sire] = x

        def dam_callback(x):
            self.dam_ids[horse.dam] = x

        sire_id = self.sire_ids.get(horse.sire) or h.send((horse.sire, sire_callback))
        dam_id = self.dam_ids.get(horse.dam) or h.send((horse.dam, dam_callback))

        return MongoHorse(**(horse.model_dump(exclude={"sire", "dam"}) | {"sire": ObjectId(sire_id), "dam": ObjectId(dam_id)}))