from functools import cache
from logging import Logger, LoggerAdapter

from clients import mongo_client as client
from models import ProcessHorse, ProcessHorseCore, PyObjectId

from processors.person_processor import person_processor

from .processor import Processor
from .utils import compact


class HorseProcessor(Processor):
    _descriptor = "horse"
    _next_processor = person_processor
    _table = client.handykapp.horses

    def _search_dictionary(self, horse: ProcessHorse | ProcessHorseCore) -> dict:
        return compact(horse.model_dump(include=["name", "country", "sex", "year"]))

    def _update_dictionary(self, horse: ProcessHorse) -> dict:
        sire = self._table.find_one(horse.sire.model_dump(), {"_id": 1})
        dam = self._table.find_one(horse.dam.model_dump(), {"_id": 1})

        return compact({
            "colour": horse.colour,
            "sire": sire["_id"] if sire else None,
            "dam": dam["_id"] if dam else None
        })

    def _insert_dictionary(self, horse: ProcessHorse) -> dict:
        return compact(self._search_dictionary(horse) | self._update_dictionary(horse))
    
    def post_process(self, horse: ProcessHorse, db_id: PyObjectId, logger: Logger | LoggerAdapter):
        if (race_id := horse["race_id"]):
            client.handykapp.races.update_one(
                {"_id": race_id},
                {
                    "$push": {
                        "runners": compact({
                            "horse": db_id,
                            "owner": horse.get("owner"),
                            "allowance": horse.get("allowance"),
                            "saddlecloth": horse.get("saddlecloth"),
                            "draw": horse.get("draw"),
                            "headgear": horse.get("headgear"),
                            "lbs_carried": horse.get("lbs_carried"),
                            "official_rating": horse.get("official_rating"),
                            "position": horse.get("position"),
                            "distance_beaten": horse.get("distance_beaten"),
                            "sp": horse.get("sp"),
                        })
                    }
                },
            )

            if horse.get("trainer"):
                person_processor.send((
                    {
                        "name": horse["trainer"],
                        "role": "trainer",
                        "race_id": race_id,
                        "horse_id": db_id,
                    },
                    horse["source"],
                    # {},
                ))

            if horse.get("jockey"):
                person_processor.send((
                    {
                        "name": horse["jockey"],
                        "role": "jockey",
                        "race_id": race_id,
                        "horse_id": db_id,
                    },
                    horse["source"],
                    # {},
                ))

horse_processor = HorseProcessor().process