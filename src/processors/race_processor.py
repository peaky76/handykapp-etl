from typing import ClassVar, List

from models import PyObjectId, Race
from prefect import get_run_logger

from processors.horse_processor import HorseProcessor

from .database_processor import DatabaseProcessor
from .processor import Processor


class RaceProcessor(DatabaseProcessor):
    _forward_processors: ClassVar[List[Processor]] = [HorseProcessor]
    _search_keys: ClassVar[List[str]] = ["racecourse_id", "datetime"]
    _update_keys: ClassVar[List[str]] = ["rapid_id", "going_description"]
    
    def post_process(self, race: Race) -> None:
        try:
            for horse in race["runners"]:
                HorseProcessor().send((
                    {"name": horse["sire"], "sex": "M", "race_id": None},
                    race["source"],
                ))

                damsire = horse.get("damsire")
                if damsire:
                    HorseProcessor().send((
                        {"name": damsire, "sex": "M", "race_id": None},
                        race["source"],
                    ))
                    
                HorseProcessor().send((
                    {
                        "name": horse["dam"],
                        "sex": "F",
                        "sire": damsire,
                        "race_id": None,
                    },
                    race["source"],
                ))
                
                if self.current_id:
                    creation_dict = horse | { "race_id": self.current_id }
                    HorseProcessor().send((creation_dict, race["source"]))

        except Exception as e:
            logger = get_run_logger()
            logger.error(f"Error processing {self.current_id}: {e}")
