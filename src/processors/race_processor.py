from typing import ClassVar, Collection, List

from clients import mongo_client as client
from models import PyObjectId, Race
from prefect import get_run_logger

from processors.horse_processor import horse_processor

from .database_processor import DatabaseProcessor
from .processor import Processor


class RaceProcessor(DatabaseProcessor):
    _descriptor: ClassVar[str] = "race"
    _next_processors: ClassVar[List[Processor]] = [horse_processor]
    _table: ClassVar[Collection] = client.handykapp.races
    _search_keys: ClassVar[List[str]] = ["racecourse_id", "datetime"]
    _update_keys: ClassVar[List[str]] = ["rapid_id", "going_description"]
    
    def post_process(self, race: Race, race_id: PyObjectId) -> None:
        try:
            for horse in race["runners"]:
                horse_processor.send((
                    {"name": horse["sire"], "sex": "M", "race_id": None},
                    race["source"],
                ))

                damsire = horse.get("damsire")
                if damsire:
                    horse_processor.send((
                        {"name": damsire, "sex": "M", "race_id": None},
                        race["source"],
                    ))
                    
                horse_processor.send((
                    {
                        "name": horse["dam"],
                        "sex": "F",
                        "sire": damsire,
                        "race_id": None,
                    },
                    race["source"],
                ))
                
                if race_id:
                    creation_dict = horse | { "race_id": race_id }
                    horse_processor.send((creation_dict, race["source"]))

        except Exception as e:
            logger = get_run_logger()
            logger.error(f"Error processing {race_id}: {e}")

race_processor = RaceProcessor().process