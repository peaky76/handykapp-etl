from typing import ClassVar, List, Set

from bson import ObjectId
from models import Horse, MongoHorse
from peak_utility.listish import compact

from processors.processor import Processor

from .database_processor import DatabaseProcessor
from .horse_core_processor import HorseCoreProcessor
from .id_hook import IdHook
from .person_processor import PersonProcessor


class HorseProcessor(DatabaseProcessor[Horse, MongoHorse]):
    _db_model = MongoHorse
    _forward_processors: ClassVar[List[Processor]] = [HorseCoreProcessor(), PersonProcessor()]
    _update_keys: ClassVar[Set[str]] = {"colour", "sire", "dam"}
    _search_keys: ClassVar[Set[str]] = {"name", "country", "sex", "year"}

    def pre_process(self, horse: Horse) -> MongoHorse:
        h = self.running_processors[0]
        p = self.running_processors[1]  

        sire_id = IdHook()
        dam_id = IdHook()
        trainer_id = IdHook()

        h.send((horse.sire, sire_id))
        h.send((horse.dam, dam_id))
        p.send((horse.trainer, trainer_id))

        return MongoHorse(**(horse.model_dump() | {"sire": ObjectId(sire_id.val), "dam": ObjectId(dam_id.val), "trainer": ObjectId(trainer_id.val)}))   