from functools import cache

from clients import mongo_client as client
from prefect import get_run_logger
from pymongo.errors import DuplicateKeyError

from loaders.horse_processor import horse_processor

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
    return {
        k: v
        for k, v in {
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
        }.items()
        if v
    }


def race_processor():
    logger = get_run_logger()
    logger.info("Starting race processor")
    race_added_count = 0
    race_updated_count = 0
    race_skipped_count = 0

    h = horse_processor()
    next(h)

    try:
        while True:
            race, source = yield
            racecourse_id = get_racecourse_id(race["course"], race["surface"], race["code"], race["obstacle"])

            if racecourse_id:
                found_race = db.race.find_one(
                    {
                        "racecourse": racecourse_id,
                        "datetime": race["datetime"],
                    }
                )

                # TODO: Check race matches data
                if found_race:
                    race_id = found_race["_id"]
                    db.race.update_one(
                        {"_id": race_id},
                        {
                            "$set": {
                                "rapid_id": race.get("rapid_id"),
                                "going_description": race.get("going_description"),
                            }
                        },
                    )
                    logger.debug(f"{race['datetime']} at {race['course']} updated")
                    race_updated_count += 1
                else:
                    try:
                        race_id = db.race.insert_one(
                            make_update_dictionary(race, racecourse_id)
                        ).inserted_id
                        logger.info(
                            f"{race.get('datetime')} at {race.get('course')} added to db"
                        )
                        race_added_count += 1
                    except DuplicateKeyError:
                        logger.warning(
                            f"Duplicate race for {race['datetime']} at {race['course']}"
                        )
                        race_skipped_count += 1

                for horse in race["runners"]:
                    h.send(({"name": horse["sire"], "sex": "M", "race_id": None}, source))
                    h.send((
                        {"name": horse["damsire"], "sex": "M", "race_id": None}, source
                    ))
                    h.send((
                        {
                            "name": horse["dam"],
                            "sex": "F",
                            "sire": horse["damsire"],
                            "race_id": None,
                        },
                        source,
                    ))

                if race_id:
                    h.send(((horse | {"race_id": race_id}), source))

    except GeneratorExit:
        logger.info(
            f"Finished processing races. Updated {race_updated_count} race, added {race_added_count} races"
        )
        h.close()
