from functools import cache

from clients import mongo_client as client
from models import ProcessHorse, ProcessHorseCore, PyObjectId

from processors.person_processor import person_processor

from .processor import Processor
from .utils import compact


class HorseProcessor(Processor):
    _descriptor = "horse"
    _next_processor = person_processor
    _table = client.handykapp.horses

    @cache
    def _search_dictionary(self, horse: ProcessHorse | ProcessHorseCore) -> dict:
        if not isinstance(horse, ProcessHorse) and not isinstance(horse, ProcessHorseCore):
            raise TypeError(f"Expected ProcessHorse or ProcessHorseCore, got {type(horse)}: {horse}")

        return compact(horse.model_dump(include=["name", "country", "sex", "year"]))

    @cache
    def _update_dictionary(self, horse: ProcessHorse | ProcessHorseCore) -> dict:
        if isinstance(horse, ProcessHorseCore):
            return {}

        return compact({
            "colour": horse.colour,
            "sire": self.find(horse.sire) if horse.sire else None,
            "dam": self.find(horse.dam) if horse.dam else None
        })

    @cache
    def _insert_dictionary(self, horse: ProcessHorse | ProcessHorseCore) -> dict:
        return compact(self._search_dictionary(horse) | self._update_dictionary(horse))
            
    def post_process(self, horse: ProcessHorse | ProcessHorseCore, db_id: PyObjectId):
        if isinstance(horse, ProcessHorse) and horse.race_id:
            client.handykapp.races.update_one(
                {"_id": horse.race_id},
                {
                    "$push": {
                        "runners": compact({
                            "horse": db_id,
                            "owner": horse.owner,
                            "allowance": horse.allowance,
                            "saddlecloth": horse.saddlecloth,
                            "draw": horse.draw,
                            "headgear": horse.headgear,
                            "lbs_carried": horse.lbs_carried,
                            "official_rating": horse.official_rating,
                            "position": horse.position,
                            "distance_beaten": horse.distance_beaten,
                            "sp": horse.sp,
                        })
                    }
                },
            )

            if horse.get("trainer"):
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

            if horse.get("jockey"):
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