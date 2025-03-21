from functools import cache

from prefect import get_run_logger
from pymongo.errors import DuplicateKeyError

from clients import mongo_client as client
from models import PreMongoRunner, PyObjectId
from processors.person_processor import person_processor

from .utils import compact

db = client.handykapp


@cache
def get_horse_id_by_name_and_sex(
    name: str | None, sex: str | None
) -> PyObjectId | None:
    if not name:
        return None

    found_horse = db.horses.find_one({"name": name, "sex": sex}, {"_id": 1})

    if not found_horse:
        raise ValueError(
            f"Could not find {'fe' if sex == 'F' else ''}male horse {name}"
        )

    return found_horse["_id"]


@cache
def get_dam_id(name: str | None) -> PyObjectId | None:
    return get_horse_id_by_name_and_sex(name, "F")


@cache
def get_sire_id(name: str | None) -> PyObjectId | None:
    return get_horse_id_by_name_and_sex(name, "M")


def make_search_dictionary(horse: PreMongoRunner) -> dict[str, str]:
    return compact(
        {
            "name": horse.name,
            "country": horse.country,
            "year": horse.year,
            "sex": horse.sex,
        }
    )


def make_update_dictionary(horse):
    update_dictionary = {}
    if colour := horse.get("colour"):
        update_dictionary["colour"] = colour
    if horse.get("sire"):
        update_dictionary["sire"] = get_sire_id(horse["sire"])
    if horse.get("dam"):
        update_dictionary["dam"] = get_dam_id(horse["dam"])
    return update_dictionary


def runner_processor():
    logger = get_run_logger()
    logger.info("Starting runner processor")
    added_count = 0
    updated_count = 0
    skipped_count = 0

    p = person_processor()
    next(p)

    try:
        while True:
            horse, race_id, source = yield

            found_horse = db.horses.find_one(make_search_dictionary(horse), {"_id": 1})

            if found_horse:
                horse_id = found_horse["_id"]
                db.horses.update_one(
                    {"_id": horse_id},
                    {"$set": make_update_dictionary(horse)},
                )
                logger.debug(f"{horse.name} updated")
                updated_count += 1
            else:
                try:
                    horse_id = db.horses.insert_one(
                        compact(
                            {
                                "name": horse.name,
                                "sex": horse.sex,
                                "year": horse.year,
                                "country": horse.country,
                                "colour": horse.colour,
                                "sire": get_sire_id(horse.sire),
                                "dam": get_dam_id(horse.dam),
                            }
                        )
                    ).inserted_id
                    logger.debug(f"{horse.name} added to db")
                    added_count += 1
                except DuplicateKeyError:
                    logger.warning(
                        f"Duplicate horse: {horse.name} ({horse.country}) {horse.year} ({horse.sex})"
                    )
                    skipped_count += 1
                except ValueError as e:
                    logger.warning(e)
                    skipped_count += 1

            # Add horse to race
            if race_id:
                db.races.update_one(
                    {"_id": race_id},
                    {
                        "$push": {
                            "runners": compact(
                                {
                                    "horse": horse_id,
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
                                }
                            )
                        }
                    },
                )
                if horse.trainer:
                    p.send(
                        (
                            {
                                "name": horse.trainer,
                                "role": "trainer",
                                "race_id": race_id,
                                "runner_id": horse_id,
                            },
                            source,
                            {},
                        )
                    )
                if horse.jockey:
                    p.send(
                        (
                            {
                                "name": horse.jockey,
                                "role": "jockey",
                                "race_id": race_id,
                                "runner_id": horse_id,
                            },
                            source,
                            {},
                        )
                    )

    except GeneratorExit:
        logger.info(
            f"Finished processing runners. Updated {updated_count}, added {added_count}, skipped {skipped_count}"
        )
        p.close()
