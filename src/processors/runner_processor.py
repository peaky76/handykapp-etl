from functools import cache, wraps

from pendulum import Date
from prefect import get_run_logger
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError

from clients import mongo_client as client
from models import MongoHorse, MongoOperation, PreMongoRunner, PyObjectId
from processors.person_processor import person_processor

from .utils import compact

db = client.handykapp


def cache_if_found(func):
    cache = {}

    @wraps(func)
    def wrapper(*args, **kwargs):
        key = (args, tuple(sorted(kwargs.items())))
        if key in cache:
            return cache[key]
        result = func(*args, **kwargs)
        if result is not None:  # Only cache when we find something
            cache[key] = result
        return result

    return wrapper


@cache_if_found
def get_horse(name: str, country: str, year: int, sex: str) -> MongoHorse | None:
    return db.horses.find_one(
        compact({"name": name, "country": country, "year": year, "sex": sex}),
    )


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


def create_gelding_operation(date: Date) -> MongoOperation:
    return {
        "operation_type": "gelding",
        "date": date,
    }


def get_operations(horse: PreMongoRunner) -> list[MongoOperation] | None:
    if not horse.gelded_from:
        return None

    return [create_gelding_operation(horse.gelded_from)]


def make_operations_update(
    horse: PreMongoRunner, db_horse: MongoHorse
) -> list[MongoOperation] | None:
    if not hasattr(horse, "gelded_from") or not horse.gelded_from:
        return None

    operations = db_horse.get("operations")

    if not operations:
        return get_operations(horse)

    gelding_op = next(op for op in operations if op.get("operation_type") == "gelding")
    non_gelding_ops = [op for op in operations if op.get("operation_type") != "gelding"]

    if not gelding_op:
        return [*operations, create_gelding_operation(horse.gelded_from)]

    current_date = gelding_op.get("date")

    if current_date is None or horse.gelded_from < current_date:
        return [*non_gelding_ops, create_gelding_operation(horse.gelded_from)]

    return [operations]


def make_update_dictionary(horse: PreMongoRunner, db_horse: MongoHorse):
    return compact(
        {
            "colour": horse.colour,
            "sire": get_sire_id(horse.sire),
            "dam": get_dam_id(horse.dam),
            "operations": make_operations_update(horse, db_horse),
            "ratings": horse.ratings,
        }
    )


def runner_processor():
    logger = get_run_logger()
    logger.info("Starting runner processor")
    added_count = 0
    updated_count = 0
    skipped_count = 0

    p = person_processor()
    next(p)

    bulk_operations = []
    bulk_threshold = 100

    try:
        while True:
            horse, race_id, source = yield

            db_horse = get_horse(horse.name, horse.country, horse.year, horse.sex)
            horse_id = db_horse["_id"] if db_horse else None

            if horse_id:
                bulk_operations.append(
                    UpdateOne(
                        {"_id": horse_id},
                        {"$set": make_update_dictionary(horse, db_horse)},
                    )
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
                                "operations": get_operations(horse),
                                "ratings": horse.ratings,
                            }
                        )
                    ).inserted_id
                    logger.debug(f"{horse.name} added to db")
                    added_count += 1
                except DuplicateKeyError:
                    logger.warning(
                        f"Duplicate horse: {horse.name} ({horse.country}) {horse.year} ({horse.sex}) in race {race_id}"
                    )
                    skipped_count += 1
                except ValueError as e:
                    logger.warning(e)
                    skipped_count += 1

            # Process bulk operations when threshold reached
            if bulk_operations and len(bulk_operations) >= bulk_threshold:
                db.horses.bulk_write(bulk_operations)
                logger.debug(f"Processed {len(bulk_operations)} bulk horse operations")
                bulk_operations = []

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
                                    "finishing_position": horse.finishing_position,
                                    "official_position": horse.official_position,
                                    "beaten_distance": horse.beaten_distance,
                                    "sp": horse.sp,
                                }
                            )
                        }
                    },
                )
                for role in ("trainer", "jockey"):
                    if person_name := getattr(horse, role, None):
                        p.send(
                            (
                                {
                                    "name": person_name,
                                    "role": role,
                                    "race_id": race_id,
                                    "runner_id": horse_id,
                                },
                                source,
                            )
                        )

    except GeneratorExit:
        # Process any remaining bulk operations
        if bulk_operations:
            db.horses.bulk_write(bulk_operations)
            logger.debug(f"Processed {len(bulk_operations)} remaining bulk operations")

        logger.info(
            f"Finished processing runners. Updated {updated_count}, added {added_count}, skipped {skipped_count}"
        )
        p.close()
