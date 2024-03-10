from logging import Logger, LoggerAdapter

from clients import mongo_client as client
from models import MongoRace, ProcessRace, PyObjectId

from processors.horse_processor import horse_processor

from .processor import Processor
from .utils import compact

db = client.handykapp


def make_update_dictionary(race):
    return compact({
        "racecourse": race.get("racecourse_id"),
        "datetime": race.get("datetime"),
        "title": race.get("title"),
        "is_handicap": race.get("is_handicap"),
        "distance_description": race.get("distance_description"),
        "going_description": race.get("going_description"),
        "race_grade": race.get("race_grade"),
        "race_class": race.get("race_class") or race.get("class"),
        "age_restriction": race.get("age_restriction"),
        "rating_restriction": race.get("rating_restriction"),
        "prize": race.get("prize"),
        "rapid_id": race.get("rapid_id"),
    })


class RaceProcessor(Processor):
    _descriptor = "race"
    _next_processor = horse_processor

    def find(self, race: ProcessRace) -> PyObjectId | None:
        found_race = db.races.find_one({
            "racecourse": race["racecourse_id"],
            "datetime": race["datetime"],
        })
        return found_race["_id"] if found_race else None

    def update(self, race: ProcessRace, db_id: PyObjectId) -> None:
        # TODO: Check race matches data
        db.races.update_one(
            {"_id": db_id},
            {
                "$set": compact({
                    "rapid_id": race.get("rapid_id"),
                    "going_description": race.get("going_description"),
                })
            },
        )

    def insert(self, race: ProcessRace) -> MongoRace:
        return db.races.insert_one(
                    make_update_dictionary(race)
                )

    def post_process(self, race: ProcessRace, race_id: PyObjectId, logger: Logger | LoggerAdapter) -> None:
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
            logger.error(f"Error processing {race_id}: {e}")

race_processor = RaceProcessor().process