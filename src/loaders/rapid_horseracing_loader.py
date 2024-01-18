# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import petl
import tomllib
from clients import mongo_client as client
from helpers import get_files, log_validation_problem, read_file
from prefect import flow, get_run_logger
from transformers.rapid_horseracing_transformer import (
    transform_results,
    validate_results,
)

from loaders.race_processor import race_processor

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["rapid_horseracing"]["spaces_dir"]

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
