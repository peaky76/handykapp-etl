from functools import cache

from clients import mongo_client as client
from pymongo.errors import DuplicateKeyError

from processors.horse_processor import horse_processor

from .processor import Processor
from .utils import compact

db = client.handykapp


@cache
def get_racecourse_id(course, surface, code, obstacle) -> str:
    surface_options = ["Tapeta", "Polytrack"] if surface == "AW" else ["Turf"]
    racecourse = db.racecourses.find_one(
        {
            "name": course.title(),
            "surface": {"$in": surface_options},
            "code": code,
            "obstacle": obstacle,
        },
        {"_id": 1},
    )
    return racecourse["_id"] if racecourse else None


def make_update_dictionary(race, racecourse_id):
    return compact({
        "racecourse": racecourse_id,
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

def race_updater(racecourse_id, race, source):
    found_race = db.races.find_one({
        "racecourse": racecourse_id,
        "datetime": race["datetime"],
    })

    # TODO: Check race matches data
    if found_race:
        race_id = found_race["_id"]
        db.races.update_one(
            {"_id": race_id},
            {
                "$set": compact({
                    "rapid_id": race.get("rapid_id"),
                    "going_description": race.get("going_description"),
                })
            },
        )
        return race_id

    return None

class RaceProcessor(Processor):
    _descriptor = "races"
    _next_processor = horse_processor

    def _process_func(self, race, source, logger, next_processor):
        added_count = 0
        updated_count = 0
        skipped_count = 0

        racecourse_id = get_racecourse_id(
            race["course"], race["surface"], race["code"], race["obstacle"]
        )

        if racecourse_id:
            if (race_id := race_updater(racecourse_id, race, source)):
                logger.debug(f"{race['datetime']} at {race['course']} updated")
                updated_count += 1
            else:
                try:
                    race_id = db.races.insert_one(
                        make_update_dictionary(race, racecourse_id)
                    ).inserted_id
                    logger.debug(
                        f"{race.get('datetime')} at {race.get('course')} added to db"
                    )
                    added_count += 1
                except DuplicateKeyError:
                    logger.warning(
                        f"Duplicate race for {race['datetime']} at {race['course']}"
                    )
                    skipped_count += 1

            try:
                for horse in race["runners"]:
                    next_processor.send((
                        {"name": horse["sire"], "sex": "M", "race_id": None},
                        source,
                    ))

                    damsire = horse.get("damsire")
                    if damsire:
                        next_processor.send((
                            {"name": damsire, "sex": "M", "race_id": None},
                            source,
                        ))
                        
                    next_processor.send((
                        {
                            "name": horse["dam"],
                            "sex": "F",
                            "sire": damsire,
                            "race_id": None,
                        },
                        source,
                    ))
                    
                    if race_id:
                        creation_dict = horse | { "race_id": race_id }
                        next_processor.send((creation_dict, source))

            except Exception as e:
                logger.error(f"Error processing {race_id}: {e}")

        return added_count, updated_count, skipped_count

race_processor = RaceProcessor().process