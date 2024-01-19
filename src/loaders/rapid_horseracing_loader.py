# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


import tomllib
from clients import mongo_client as client
from helpers import get_files, read_file
from prefect import flow, get_run_logger
from transformers.rapid_horseracing_transformer import (
    transform_results,
    validate_results,
)

from loaders.dicts_processor import dicts_processor

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["rapid_horseracing"]["spaces_dir"]

db = client.handykapp


@flow
def load_rapid_horseracing_data():
    logger = get_run_logger()
    logger.info("Starting rapid_horseracing loader")

    d = dicts_processor()
    next(d)

    files = get_files(f"{SOURCE}results")

    for file in files:
        if file != "results_to_do_list.json":
            contents = read_file(file)
            d.send(([contents], validate_results, transform_results, file, "rapid"))

    d.close()


if __name__ == "__main__":
    load_rapid_horseracing_data()
