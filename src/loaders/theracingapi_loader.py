# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import petl  # type: ignore
import tomllib
from clients import mongo_client as client
from helpers import get_files, log_validation_problem, read_file
from prefect import flow, get_run_logger
from transformers.theracingapi_transformer import transform_races, validate_races

from loaders.race_processor import race_processor

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]

db = client.handykapp
            
def file_processor():
    logger = get_run_logger()
    logger.info("Starting theracingapi transformer")
    reject_count = 0
    transform_count = 0

    r = race_processor()
    next(r)

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
                    r.send(race)

                transform_count += 1
                if transform_count % 10 == 0:
                    logger.info(f"Read {transform_count} days of racecards")

    except GeneratorExit:
        logger.info(
            f"Finished processing {transform_count} days of racecards, rejected {reject_count}"
        )
        r.close()


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
