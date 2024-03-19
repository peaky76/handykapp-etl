from typing import ClassVar, List, Set

from models import Declaration, MongoRace
from prefect import get_run_logger

from processors.processor import Processor

from .database_processor import DatabaseProcessor
from .horse_processor import HorseProcessor


class RaceProcessor(DatabaseProcessor[Declaration, MongoRace]):
    _db_model = MongoRace
    _forward_processors: ClassVar[List[Processor]] = [HorseProcessor()]
    _search_keys: ClassVar[Set[str]] = {"racecourse_id", "datetime"}
    _update_keys: ClassVar[Set[str]] = {"rapid_id", "going_description"}
    
    def post_process(self, race: Declaration) -> None:
        h = self.running_processors[0]
        try:
            for horse in race.runners:
                h.send(horse.sire)
                if horse.damsire:
                    h.send(horse.damsire)
                h.send(horse.dam) 
                h.send(horse)

        except Exception as e:
            logger = get_run_logger()
            logger.error(f"Error processing {self.current_id}: {e}")
