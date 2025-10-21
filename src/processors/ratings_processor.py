from collections.abc import Generator

from prefect import get_run_logger
from pymongo import UpdateOne

from clients import mongo_client as client
from clients.mongo_client import get_horse
from models import PreMongoHorse

db = client.handykapp


def ratings_processor() -> Generator[None, PreMongoHorse, None]:
    logger = get_run_logger()
    logger.info("Starting ratings processor")
    updated_count = 0
    skipped_count = 0

    bulk_operations = []
    bulk_threshold = 50

    try:
        while True:
            horse = yield

            try:
                horse_doc = get_horse(horse)

                if not horse_doc:
                    skipped_count += 1
                    continue

                bulk_operations.append(
                    UpdateOne(
                        {"_id": horse_doc["_id"]},
                        {"$set": {"ratings": horse.ratings.model_dump()}},
                    )
                )
                updated_count += 1
            except Exception:
                skipped_count += 1

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
            f"Finished processing ratings. Updated {updated_count}, skipped {skipped_count}"
        )
