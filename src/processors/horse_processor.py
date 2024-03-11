from functools import cache

from clients import mongo_client as client
from models import PyObjectId, TransformedHorse, TransformedHorseCore, TransformedRunner

from processors.person_processor import person_processor

from .processor import Processor
from .utils import compact


class HorseProcessor(Processor):
    _descriptor = "horse"
    _next_processor = person_processor
    _table = client.handykapp.horses

    @cache
    def _search_dictionary(self, horse: TransformedHorse | TransformedHorseCore) -> dict:
        if not isinstance(horse, TransformedHorse) and not isinstance(horse, TransformedHorseCore):
            raise TypeError(f"Expected TransformedHorse or TransformedHorseCore, got {type(horse)}: {horse}")

        return compact(horse.model_dump(include=["name", "country", "sex", "year"]))

    @cache
    def _update_dictionary(self, horse: TransformedHorse | TransformedHorseCore) -> dict:
        if isinstance(horse, TransformedHorseCore):
            return {}

        return compact({
            "colour": horse.colour,
            "sire": self.find(horse.sire) if horse.sire else None,
            "dam": self.find(horse.dam) if horse.dam else None
        })

    @cache
    def _insert_dictionary(self, horse: TransformedHorse | TransformedHorseCore) -> dict:
        return compact(self._search_dictionary(horse) | self._update_dictionary(horse))
            
    def post_process(self, horse: TransformedHorse | TransformedHorseCore, db_id: PyObjectId):
        if isinstance(horse, TransformedRunner) and horse.race_id:
            client.handykapp.races.update_one(
                {"_id": horse.race_id},
                {
                    "$push": {
                        "runners": {"horse": horse.db_id } | 
                            compact(horse.model_dump(include=[
                                "owner",
                                "allowance",
                                "saddlecloth",
                                "draw",
                                "headgear",
                                "lbs_carried",
                                "official_rating",
                                "position",
                                "distance_beaten",
                                "sp",
                        ]))
                    }
                },
            )

            if horse.trainer:
                person_processor.send((
                    {
                        "name": horse["trainer"],
                        "role": "trainer",
                        "race_id": horse.race_id,
                        "horse_id": db_id,
                    },
                    horse["source"],
                    # {},
                ))

            if horse.jockey:
                person_processor.send((
                    {
                        "name": horse["jockey"],
                        "role": "jockey",
                        "race_id": horse.race_id,
                        "horse_id": db_id,
                    },
                    horse["source"],
                    # {},
                ))

horse_processor = HorseProcessor().process