from collections.abc import Generator
from functools import cache

import pendulum
from prefect import get_run_logger
from pymongo.errors import DuplicateKeyError

from clients import mongo_client as client
from models import PreMongoRace
from models.pre_mongo_runner import PreMongoRunner
from processors.runner_processor import runner_processor
from transformers.validators import ensure_datetime

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
def get_all_racecourses():
    return list(
        db.racecourses.find(
            {},
            {
                "name": 1,
                "formal_name": 1,
                "surface": 1,
                "code": 1,
                "obstacle": 1,
                "references": 1,
            },
        )
    )


def get_racecourse_id(race: PreMongoRace, source: str) -> str | None:
    if source == "racing_research":
        return rr_code_to_course_dict().get(race.course)

    racecourses = get_all_racecourses()
    surface_options = ["Tapeta", "Polytrack"] if race.surface == "AW" else ["Turf"]

    for racecourse in racecourses:
        if (
            (
                racecourse["name"].lower() == race.course.lower()
                or racecourse["formal_name"].lower() == race.course.lower()
            )
            and racecourse.get("surface") in surface_options
            and racecourse.get("code") == race.code
            and (
                racecourse.get("obstacle") == race.obstacle
                or (racecourse.get("obstacle") is None and race.obstacle is None)
            )
        ):
            return racecourse["_id"]

    return None


def make_update_dictionary(race, racecourse_id) -> PreMongoRace:
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


def race_processor() -> Generator[None, tuple[PreMongoRace, str], None]:
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

            log_description = f"{ensure_datetime(pendulum.parse(str(race.datetime))).format('Do MMM YYYY HH:mm')} {race.title} at {race.course} from {source}"

            if racecourse_id := get_racecourse_id(race, source):
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
                                    PreMongoRunner(name=horse.sire, sex="M"),
                                    None,
                                    source,
                                )
                            )

                        if horse.damsire:
                            r.send(
                                (
                                    PreMongoRunner(name=horse.damsire, sex="M"),
                                    None,
                                    source,
                                )
                            )

                        if horse.dam:
                            r.send(
                                (
                                    PreMongoRunner(
                                        name=horse.dam, sex="F", sire=horse.damsire
                                    ),
                                    None,
                                    source,
                                )
                            )

                        if race_id:
                            r.send((horse, race_id, source))
                except Exception as e:
                    logger.error(f"Error processing {log_description}: {e}")
            else:
                logger.error(
                    f"Unable to add {log_description}: No matching racecourse found"
                )

    except GeneratorExit:
        logger.info(
            f"Finished processing races. Updated {updated_count} races, added {added_count}, skipped {skipped_count}"
        )
        r.close()
