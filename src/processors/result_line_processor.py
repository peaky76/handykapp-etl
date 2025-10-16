from collections.abc import Generator

from prefect import get_run_logger

from clients import mongo_client as client
from models import FormdataHorse, FormdataRun
from processors.race_processor import rr_code_to_course_dict

db = client.handykapp


def result_line_processor() -> Generator[None, tuple[FormdataHorse, FormdataRun], None]:
    logger = get_run_logger()
    logger.info("Starting result line processor")
    updated_count = 0
    skipped_count = 0

    try:
        while True:
            horse, run = yield

            racecourse_id = rr_code_to_course_dict().get(run.course)

            found_horse = db.horses.find_one(
                {"name": horse.name, "country": horse.country, "year": horse.year},
                {"_id": 1},
            )

            if not found_horse:
                logger.warning(f"Horse {horse.name} not found in db, skipping result")
                skipped_count += 1
                continue

            found_race = db.races.find_one(
                {
                    "racecourse": racecourse_id,
                    "$expr": {
                        "$eq": [
                            {
                                "$dateToString": {
                                    "format": "%Y-%m-%d",
                                    "date": "$datetime",
                                }
                            },
                            run.date.strftime("%Y-%m-%d"),
                        ]
                    },
                    "runners.horse": found_horse["_id"],
                }
            )

            if found_race:
                race_id = found_race["_id"]
                db.races.update_one(
                    {"_id": race_id, "runners.horse": found_horse["_id"]},
                    {
                        "$set": {
                            "going": run.going,
                            "runners.$.finishing_position": run.position,
                            "runners.$.beaten_distance": run.beaten_distance,
                        }
                    },
                )
                logger.debug(
                    f"Added result for {horse.name} in race at {run.course} on {run.date}"
                )
    except GeneratorExit:
        logger.info(
            f"Finished processing result lines. Added results for {updated_count} runs, skipped {skipped_count}"
        )
