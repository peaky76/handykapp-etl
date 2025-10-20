from collections.abc import Generator

from peak_utility.listish import compact
from prefect import get_run_logger
from pymongo import UpdateOne
from pymongo.errors import DuplicateKeyError

from clients.mongo_client import get_horse, mongo_client
from models import PreMongoRunner, PyObjectId
from processors.horse_processor import (
    make_horse_insert_dictionary,
    make_horse_update_dictionary,
)
from processors.person_processor import person_processor

db = mongo_client.handykapp


def runner_processor() -> Generator[None, tuple[PreMongoRunner, PyObjectId, str], None]:
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

            db_horse = get_horse(horse)
            horse_id = db_horse["_id"] if db_horse else None

            if horse_id:
                bulk_operations.append(
                    UpdateOne(
                        {"_id": horse_id},
                        {"$set": make_horse_update_dictionary(horse, db_horse)},
                    )
                )
                logger.debug(f"{horse.name} updated")
                updated_count += 1
            else:
                try:
                    horse_id = db.horses.insert_one(
                        make_horse_insert_dictionary(horse)
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
