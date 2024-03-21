from typing import ClassVar, List, Set

from bson import ObjectId
from models import Horse, MongoHorse

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

        if horse.sire:
            h.send((horse.sire, sire_id))
        
        if horse.dam:
            h.send((horse.dam, dam_id))
        
        if horse.trainer:
            p.send((horse.trainer, trainer_id))

        return MongoHorse(**(horse.model_dump() | {
            "sire": ObjectId(sire_id.val) if sire_id.val else None, 
            "dam": ObjectId(dam_id.val) if dam_id.val else None, 
            "trainer": ObjectId(trainer_id.val) if trainer_id.val else None
        }))   