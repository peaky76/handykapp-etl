from collections.abc import Generator

from peak_utility.listish import compact
from prefect import get_run_logger
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError

from clients.mongo_client import get_horse, mongo_client
from helpers.helpers import get_operations, make_operations_update
from models import MongoHorse, PreMongoHorse, PreMongoRunner

db = mongo_client.handykapp


def make_insert_dictionary(horse: PreMongoHorse):
    return compact(
        {
            "name": horse.name,
            "sex": horse.sex,
            "year": horse.year,
            "country": horse.country,
            "colour": horse.colour,
            "sire": x["_id"] if (x := get_horse(horse.sire)) else None,
            "dam": x["_id"] if (x := get_horse(horse.dam)) else None,
            "operations": get_operations(horse),
            "ratings": horse.ratings,
        }
    )


def make_update_dictionary(horse: PreMongoRunner, db_horse: MongoHorse):
    return compact(
        {
            "colour": horse.colour,
            "sire": x["_id"] if (x := get_horse(horse.sire)) else None,
            "dam": x["_id"] if (x := get_horse(horse.dam)) else None,
            "operations": make_operations_update(horse, db_horse),
            "ratings": horse.ratings,
        }
    )


def horse_processor() -> Generator[None, PreMongoHorse, None]:
    logger = get_run_logger()
    logger.info("Starting runner processor")
    added_count = 0
    updated_count = 0
    skipped_count = 0

    bulk_operations = []
    bulk_threshold = 100

    try:
        while True:
            horse = yield

            db_horse = get_horse(horse)
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
                    db.horses.insert_one(make_insert_dictionary(horse))
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

            # Process bulk operations when threshold reached
            if bulk_operations and len(bulk_operations) >= bulk_threshold:
                db.horses.bulk_write(bulk_operations)
                logger.debug(f"Processed {len(bulk_operations)} bulk horse operations")
                bulk_operations = []

    except GeneratorExit:
        # Process any remaining bulk operations
        if bulk_operations:
            db.horses.bulk_write(bulk_operations)
            logger.debug(f"Processed {len(bulk_operations)} remaining bulk operations")

        logger.info(
            f"Finished processing horses. Updated {updated_count}, added {added_count}, skipped {skipped_count}"
        )
