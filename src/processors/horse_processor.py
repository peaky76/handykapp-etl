from functools import cache
from logging import Logger, LoggerAdapter

from clients import mongo_client as client
from models import MongoHorse, ProcessHorse, PyObjectId

from processors.person_processor import person_processor

from .processor import Processor
from .utils import compact

db = client.handykapp


@cache
def get_horse_id_by_name_and_sex(name: str | None, sex: str | None) -> PyObjectId:
    if not name:
        return None

    found_horse = db.horses.find_one({"name": name, "sex": sex}, {"_id": 1})

    if not found_horse:
        raise ValueError(
            f"Could not find {'fe' if sex == 'F' else ''}male horse {name}"
        )

    return found_horse["_id"]


@cache
def get_dam_id(name: str | None) -> PyObjectId:
    return get_horse_id_by_name_and_sex(name, "F")


@cache
def get_sire_id(name: str | None) -> PyObjectId:
    return get_horse_id_by_name_and_sex(name, "M")


def make_search_dictionary(horse) -> MongoHorse:
    keys = ["name", "country", "year"] if horse.get("country") else ["name", "sex"]

    return {k: horse[k] for k in keys}


def make_update_dictionary(horse):
    return compact({
        "colour": horse.get("colour"),
        "sire": get_sire_id(horse.get("sire")),
        "dam": get_dam_id(horse.get("dam")),
    })


class HorseProcessor(Processor):
    _descriptor = "horse"
    _next_processor = person_processor

    def find(self, horse: ProcessHorse) -> PyObjectId | None:
        found_horse = db.horses.find_one(make_search_dictionary(horse), {"_id": 1})
        return found_horse["_id"] if found_horse else None

    def update(self, horse: ProcessHorse, db_id: PyObjectId) -> None:
        db.horses.update_one(
            {"_id": db_id},
            {"$set": make_update_dictionary(horse)},
        )
    
    def insert(self, horse: ProcessHorse) -> PyObjectId:
        return db.horses.insert_one(
                    compact({
                        "name": horse.get("name"),
                        "sex": horse.get("sex"),
                        "year": horse.get("year"),
                        "country": horse.get("country"),
                        "colour": horse.get("colour"),
                        "sire": get_sire_id(horse.get("sire")),
                        "dam": get_dam_id(horse.get("dam")),
                    })
                ).inserted_id

    def post_process(self, horse: ProcessHorse, db_id: PyObjectId, logger: Logger | LoggerAdapter):
        if (race_id := horse["race_id"]):
            db.races.update_one(
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