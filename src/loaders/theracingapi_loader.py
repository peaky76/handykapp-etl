# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import petl  # type: ignore
import tomllib
from clients import mongo_client as client
from helpers import get_files, log_validation_problem, read_file
from prefect import flow, get_run_logger
from pymongo.errors import DuplicateKeyError
from transformers.theracingapi_transformer import transform_races, validate_races

from loaders.getters import lookup_racecourse_id
from loaders.horse_processor import horse_processor

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]

db = client.handykapp


def declaration_processor():
    logger = get_run_logger()
    logger.info("Starting declaration processor")
    racecourse_ids = {}
    # racecourse_adds_count = 0
    declaration_adds_count = 0
    declaration_skips_count = 0

    h = horse_processor()
    next(h)

    try:
        while True:
            dec = yield
            racecourse_id = lookup_racecourse_id(
                dec["course"],
                dec["surface"],
                dec["code"],
                dec["obstacle"],
            )

            if racecourse_id:
                try:
                    declaration = db.races.insert_one({
                        "racecourse": racecourse_id,
                        "datetime": dec["datetime"],
                        "title": dec["title"],
                        "is_handicap": dec["is_handicap"],
                        "distance_description": dec["distance_description"],
                        "race_grade": dec["race_grade"],
                        "race_class": dec["race_class"],
                        "age_restriction": dec["age_restriction"],
                        "rating_restriction": dec["rating_restriction"],
                        "prize": dec["prize"],
                    })
                    declaration_adds_count += 1

                    for horse in dec["runners"]:
                        h.send({"name": horse["sire"], "sex": "M", "race_id": None})
                        h.send({"name": horse["damsire"], "sex": "M", "race_id": None})
                        h.send({
                            "name": horse["dam"],
                            "sex": "F",
                            "sire": horse["damsire"],
                            "race_id": None,
                        })
                        h.send(horse | {"race_id": declaration.inserted_id})
                except DuplicateKeyError:
                    logger.warning(
                        f"Duplicate declaration for {dec['datetime']} at {dec['course']}"
                    )
            else:
                declaration_skips_count += 1

    except GeneratorExit:
        logger.info(
            f"Finished processing declarations. Added {declaration_adds_count} declarations, skipped {declaration_skips_count}"
        )
        h.close()

def file_processor():
    logger = get_run_logger()
    logger.info("Starting theracingapi transformer")
    reject_count = 0
    transform_count = 0

    d = declaration_processor()
    next(d)

    try:
        while True:
            file = yield
            day = read_file(file)

            racecards = petl.fromdicts(
                {k: v for k, v in dec.items() if k != "off_dt"}
                for dec in day["racecards"]
            )
            problems = validate_races(racecards)

            if len(problems.dicts()) > 0:
                logger.warning(f"Validation p3roblems in {file}")
                if len(problems.dicts()) > 10:
                    logger.warning("Too many problems to log")
                else:
                    for problem in problems.dicts():
                        log_validation_problem(problem)
                reject_count += 1
            else:
                races = transform_races(racecards)
                for race in races:
                    d.send(race)

                transform_count += 1
                if transform_count % 10 == 0:
                    logger.info(f"Read {transform_count} days of racecards")

    except GeneratorExit:
        logger.info(
            f"Finished processing {transform_count} days of racecards, rejected {reject_count}"
        )
        d.close()


@flow
def load_theracingapi_data():
    logger = get_run_logger()
    logger.info("Starting theracingapi loader")

    f = file_processor()
    next(f)
    for file in get_files(f"{SOURCE}racecards"):
        f.send(file)

    f.close()


if __name__ == "__main__":
    load_theracingapi_data()
