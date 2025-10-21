from functools import cache

from prefect import get_run_logger

# from pymongo import InsertOne, UpdateOne
from clients import mongo_client as client
from models import MongoHorse

# from models.formdata_horse import FormdataHorse
from .result_line_processor import result_line_processor

db = client.handykapp


@cache
def find_horse(name: str, country: str, year: int) -> MongoHorse | None:
    """Find horse by name, handling punctuation differences."""
    # First try exact match
    result = db.horses.find_one(
        {"name": name, "country": country, "year": year},
    )

    if result:
        return MongoHorse.model_validate(result)

    # If no exact match, try regex that allows apostrophes in db names
    # Convert "JOHNS BOY" to pattern that matches "JOHN'S BOY"
    # Insert optional apostrophe after each character
    pattern = ""
    for char in name:
        if char.isalpha():
            pattern += char + "'?"
        else:
            pattern += char

    result = db.horses.find_one(
        {
            "name": {"$regex": f"^{pattern}$", "$options": "i"},
            "country": country,
            "year": year,
        },
    )

    return MongoHorse.model_validate(result) if result else None


def entry_processor():
    logger = get_run_logger()
    logger.info("Starting entry processor")

    bulk_operations = []
    # bulk_threshold = 50
    updated_count = 0
    skipped_count = 0
    processed_count = 0

    rl = result_line_processor()
    next(rl)

    try:
        while True:
            horse = yield
            processed_count += 1

            # Formdata table processing

            # existing_entry = db.formdata.find_one(
            #     {
            #         "name": horse.name,
            #         "country": horse.country,
            #         "year": horse.year,
            #     }
            # )

            # if existing_entry:
            #     existing_horse = FormdataHorse.model_validate(existing_entry)
            #     runs = existing_horse.runs

            #     for new_run in horse.runs:
            #         matched_run = next(
            #             (r for r in runs if r.date == new_run.date),
            #             None,
            #         )
            #         if matched_run:
            #             runs.remove(matched_run)
            #         runs.append(new_run)

            #     bulk_operations.append(
            #         UpdateOne(
            #             {
            #                 "name": horse.name,
            #                 "country": horse.country,
            #                 "year": horse.year,
            #             },
            #             {
            #                 "$set": {
            #                     "runs": [run.model_dump() for run in runs],
            #                     "prize_money": horse.prize_money,
            #                     "trainer": horse.trainer,
            #                     "trainer_form": horse.trainer_form,
            #                 }
            #             },
            #         )
            #     )
            # else:
            #     bulk_operations.append(InsertOne(horse.model_dump()))

            # # Execute bulk operations when threshold is reached
            # if len(bulk_operations) >= bulk_threshold:
            #     db.formdata.bulk_write(bulk_operations)
            #     bulk_operations = []

            # if processed_count % 100 == 0:
            #     logger.info(f"Processed {processed_count} horses into Formdata table")

            # Result line processing
            found_horse = find_horse(horse.name, horse.country, horse.year)

            if not found_horse:
                logger.warning(
                    f"Horse {horse.name} {horse.country} {horse.year} not found in db, skipping result"
                )
                skipped_count += 1
            else:
                for run in horse.runs:
                    rl.send((found_horse, run))
                updated_count += 1

    except GeneratorExit:
        # Process remaining operations
        if bulk_operations:
            db.formdata.bulk_write(bulk_operations)
        logger.info(
            f"Completed processing {processed_count} horses into Formdata table. Updated {updated_count} horses in Horses table with results. Skipped {skipped_count} unfound horses."
        )
