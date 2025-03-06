from functools import cache

from prefect import get_run_logger
from pymongo.errors import DuplicateKeyError

from clients import mongo_client as client
from models import MongoRace
from processors.runner_processor import runner_processor

from .utils import compact

db = client.handykapp


@cache
def rr_code_to_course_dict():
    source = db.racecourses.find(
        projection={"_id": 2, "references": {"racing_research": 1}}
    )
    return {
        racecourse["references"]["racing_research"]: racecourse["_id"]
        for racecourse in source
    }


@cache
def get_racecourse_id(race: MongoRace) -> str | None:
    surface_options = ["Tapeta", "Polytrack"] if race.surface == "AW" else ["Turf"]
    racecourse = db.racecourses.find_one(
        {
            "name": race.course.title(),
            "surface": {"$in": surface_options},
            "code": race.code,
            "obstacle": race.obstacle,
        },
        {"_id": 1},
    )

    return (
        racecourse["_id"]
        if racecourse
        else rr
        if (rr := rr_code_to_course_dict()[race.course])
        else None
    )


def make_update_dictionary(race, racecourse_id) -> MongoRace:
    return compact(
        {
            "racecourse": racecourse_id,
            "datetime": race.datetime,
            "title": race.title,
            "is_handicap": race.is_handicap,
            "distance_description": race.distance_description,
            "going_description": race.going_description,
            "race_grade": race.race_grade,
            "race_class": race.race_class,
            "age_restriction": race.age_restriction,
            "rating_restriction": race.rating_restriction,
            "prize": race.prize,
            "rapid_id": race.rapid_id,
        }
    )


def race_processor():
    logger = get_run_logger()
    logger.info("Starting race processor")
    added_count = 0
    updated_count = 0
    skipped_count = 0

    r = runner_processor()
    next(r)

    try:
        while True:
            race, source = yield

            if racecourse_id := get_racecourse_id(race):
                found_race = db.races.find_one(
                    {
                        "racecourse": racecourse_id,
                        "datetime": race.datetime,
                    }
                )

                # TODO: Check race matches data
                if found_race:
                    race_id = found_race["_id"]
                    db.races.update_one(
                        {"_id": race_id},
                        {
                            "$set": compact(
                                {
                                    "rapid_id": race.rapid_id,
                                    "going_description": race.going_description,
                                }
                            )
                        },
                    )
                    logger.debug(f"{race.datetime} at {race.course} updated")
                    updated_count += 1
                else:
                    try:
                        race_id = db.races.insert_one(
                            make_update_dictionary(race, racecourse_id)
                        ).inserted_id
                        logger.debug(f"{race.datetime} at {race.course} added to db")
                        added_count += 1
                    except DuplicateKeyError:
                        logger.warning(
                            f"Duplicate race for {race.datetime} at {race.course}"
                        )
                        skipped_count += 1

                try:
                    for horse in race.runners:
                        if horse.sire:
                            r.send(
                                (
                                    {"name": horse.sire, "sex": "M", "race_id": None},
                                    source,
                                )
                            )

                        if horse.damsire:
                            r.send(
                                (
                                    {
                                        "name": horse.damsire,
                                        "sex": "M",
                                        "race_id": None,
                                    },
                                    source,
                                )
                            )

                        if horse.dam:
                            r.send(
                                (
                                    {
                                        "name": horse.dam,
                                        "sex": "F",
                                        "sire": horse.damsire,
                                        "race_id": None,
                                    },
                                    source,
                                )
                            )

                        if race_id:
                            r.send((horse | {"race_id": race_id}, source))
                except Exception as e:
                    logger.error(f"Error processing {race_id}: {e}")
            else:
                logger.error(
                    f"Unable to add race {race_id}: No matching racecourse found"
                )

    except GeneratorExit:
        logger.info(
            f"Finished processing races. Updated {updated_count} races, added {added_count}, skipped {skipped_count}"
        )
        r.close()
