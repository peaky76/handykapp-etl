# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))



import petl
import tomllib
from clients import mongo_client as client
from helpers import get_files, log_validation_problem, read_file
from prefect import flow, get_run_logger
from pymongo.errors import DuplicateKeyError
from transformers.rapid_horseracing_transformer import (
    transform_results,
    validate_results,
)

from loaders.getters import lookup_racecourse_id
from loaders.horse_processor import horse_processor

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["rapid_horseracing"]["spaces_dir"]

db = client.handykapp


def result_processor():
    logger = get_run_logger()
    logger.info("Starting result processor")
    racecourse_ids = {}
    race_added_count = 0
    race_updated_count = 0
    result_skips_count = 0

    h = horse_processor()
    next(h)

    try:
        while True:
            result = yield
            racecourse_id = lookup_racecourse_id(
                result["course"],
                result["surface"],
                result["code"],
                result["obstacle"],
            )

            if racecourse_id:
                race = db.race.find_one({
                    "racecourse": racecourse_id,
                    "datetime": result["datetime"],
                })

                # TODO: Check race matches rapid_horseracing data
                if race:
                    race_id = race["_id"]
                    db.race.update_one(
                        {"_id": race_id},
                        {
                            "$set": {
                                "rapid_id": result["rapid_id"],
                                "going_description": result["going_description"],
                            }
                        },
                    )
                    logger.debug(f"{result['datetime']} at {result['course']} updated")
                    race_updated_count += 1
                else:
                    try:
                        race_id = db.race.insert_one({
                            "racecourse": racecourse_id,
                            "datetime": result["datetime"],
                            "title": result["title"],
                            "is_handicap": result["is_handicap"],
                            "distance_description": result["distance_description"],
                            "going_description": result["going_description"],
                            "race_class": result["class"],
                            "age_restriction": result["age_restriction"],
                            "prize": result["prize"],
                            "rapid_id": result["rapid_id"],
                        })["inserted_id"]
                        logger.info(
                            f"{result['datetime']} at {result['course']} added to db"
                        )
                        race_added_count += 1
                    except DuplicateKeyError:
                        logger.warning(
                            f"Duplicate result for {result['datetime']} at {result['course']}"
                        )
                        result_skips_count += 1

                for horse in result["runners"]:
                    h.send({"name": horse["sire"], "sex": "M", "race_id": None})
                    h.send({"name": horse["damsire"], "sex": "M", "race_id": None})
                    h.send({
                        "name": horse["dam"],
                        "sex": "F",
                        "sire": horse["damsire"],
                        "race_id": None,
                    })

                if race_id:
                    h.send(horse | {"race_id": race_id})

    except GeneratorExit:
        logger.info(
            f"Finished processing results. Updated {race_updated_count} race, added {race_added_count} results"
        )
        h.close()


def file_processor():
    logger = get_run_logger()
    logger.info("Starting theracingapi transformer")
    reject_count = 0
    transform_count = 0

    r = result_processor()
    next(r)

    try:
        while True:
            file = yield
            result = [read_file(file)]
            data = petl.fromdicts(result)

            problems = validate_results(data)

            if len(problems.dicts()) > 0:
                logger.warning(f"Validation problems in {file}")
                if len(problems.dicts()) > 10:
                    logger.warning("Too many problems to log")
                else:
                    for problem in problems.dicts():
                        log_validation_problem(problem)
                reject_count += 1
            else:
                results = transform_results(result)
                for race in results:
                    r.send(result)

                transform_count += 1
                if transform_count % 10 == 0:
                    logger.info(f"Read {transform_count} results")

    except GeneratorExit:
        logger.info(
            f"Finished transforming {transform_count} results, rejected {reject_count}"
        )
        r.close()


@flow
def load_rapid_horseracing_data():
    logger = get_run_logger()
    logger.info("Starting rapid_horseracing loader")

    f = file_processor()
    next(f)

    files = get_files(f"{SOURCE}results")

    for file in files:
        if file != "results_to_do_list.json":
            f.send(file) 

    f.close()

if __name__ == "__main__":
    load_rapid_horseracing_data()
