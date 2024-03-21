from typing import ClassVar, List

from bson import ObjectId
from models import Declaration, MongoRace
from models.mongo_run import MongoRun

from processors.processor import Processor

from .database_processor import DatabaseProcessor
from .horse_processor import HorseProcessor
from .id_hook import IdHook
from .person_processor import PersonProcessor
from .racecourse_processor import RacecourseProcessor


class DeclarationProcessor(DatabaseProcessor[Declaration, MongoRace]):
    _table_name: ClassVar[str] = 'races'
    _forward_processors: ClassVar[List[Processor]] = [RacecourseProcessor(), HorseProcessor(), PersonProcessor()]
    _search_keys: ClassVar[List[str]] = ["racecourse_id", "datetime"]
    _update_keys: ClassVar[List[str]] = ["rapid_id", "going_description"]

    def pre_process(self, declaration: Declaration) -> None:
        r = self.running_processors[0]
        h = self.running_processors[1]
        p = self.running_processors[2]  

        racecourse_id = IdHook()
        jockey_id = IdHook()
        trainer_id = IdHook()
        
        r.send((declaration.racecourse, racecourse_id))

        runners = []
        for horse in declaration.runners:
            horse_id = IdHook()
            h.send((horse, horse_id))
            p.send((horse.jockey, jockey_id))
            p.send((horse.trainer, trainer_id))
            runners.append(MongoRun(**(horse.model_dump() | { 
                "horse": ObjectId(horse_id.val),
                "jockey": ObjectId(jockey_id.val),
                "trainer": ObjectId(trainer_id.val)
            })))

        return MongoRace(**(declaration.model_dump() | {"racecourse": ObjectId(racecourse_id.val), "runners": runners}))   
