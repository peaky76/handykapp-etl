from collections.abc import Generator

from horsetalk import Going
from prefect import get_run_logger

from clients import mongo_client as client
from clients.mongo_client import rr_code_to_course_dict
from models import FormdataRun, MongoHorse

db = client.handykapp


def result_line_processor() -> Generator[None, tuple[MongoHorse, FormdataRun], None]:
    logger = get_run_logger()
    logger.info("Starting result line processor")

    try:
        while True:
            horse, run = yield

            surface = Going(run.going).surface
            racecourse_id = rr_code_to_course_dict().get((run.course, surface))

            if not racecourse_id:
                logger.warning(f"No racecourse found for {run.course} ({surface})")
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
                            run.date,
                        ]
                    },
                    "runners.horse": horse["_id"],
                }
            )

            if found_race:
                race_id = found_race["_id"]
                db.races.update_one(
                    {"_id": race_id, "runners.horse": horse["_id"]},
                    {
                        "$set": {
                            "going_assessment": str(Going(run.going)),
                            "runners.$.finishing_position": run.position,
                            "runners.$.beaten_distance": run.beaten_distance,
                            "runners.$.time_rating": run.time_rating,
                            "runners.$.form_rating": run.form_rating,
                        }
                    },
                )
                logger.debug(
                    f"Added result for {horse['_id']} in race at {run.course} on {run.date}"
                )
            else:
                logger.warning(
                    f"No race found for {horse['_id']} at {run.course} on {run.date}"
                )
    except GeneratorExit:
        logger.info("Finished processing results.")
