from functools import cache

import petl  # type: ignore
from clients import mongo_client as client
from helpers import log_validation_problem
from prefect import get_run_logger

from processors.race_processor import race_processor

db = client.handykapp

@cache
def get_racecourse_id(course, surface, code, obstacle) -> str:
    surface_options = ["Tapeta", "Polytrack"] if surface == "AW" else ["Turf"]
    racecourse = db.racecourses.find_one(
        {
            "name": course.title(),
            "surface": {"$in": surface_options},
            "code": code,
            "obstacle": obstacle,
        },
        {"_id": 1},
    )
    return racecourse["_id"] if racecourse else None

def record_processor():
    logger = get_run_logger()
    logger.info("Starting record processor")
    reject_count = 0
    transform_count = 0

    r = race_processor()
    next(r)

    try:
        while True:
            record, validator, transformer, filename, source = yield
            data = petl.fromdicts([record])
            problems = validator(data)

            if len(problems.dicts()) > 0:
                logger.warning(f"Validation problems in {filename}")
                if len(problems.dicts()) > 10:
                    logger.warning("Too many problems to log")
                else:
                    for problem in problems.dicts():
                        log_validation_problem(problem)
                reject_count += 1
            else:
                try:
                    results = transformer(data)
                except Exception as e:
                    logger.error(f"Error transforming {filename}: {e}")
                    reject_count += 1
                    continue

                for race in results:
                    racecourse_id = get_racecourse_id(
                        race["course"], race["surface"], race["code"], race["obstacle"]
                    )
                    creation_dict = race | { "racecourse_id": racecourse_id }
                    r.send((creation_dict, source))
                    transform_count += 1

                if transform_count % 25 == 0:
                    logger.info(
                        f"Read {transform_count} races. Current: {race['datetime']} at {race['course']}"
                    )

    except GeneratorExit:
        logger.info(
            f"Finished transforming {transform_count} races, rejected {reject_count}"
        )
        r.close()
