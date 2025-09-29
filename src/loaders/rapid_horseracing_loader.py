# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


import tomllib
from prefect import flow, get_run_logger

from clients import SpacesClient
from clients import mongo_client as client
from models import RapidRecord
from processors.record_processor import record_processor
from transformers.rapid_horseracing_transformer import transform_results

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["rapid_horseracing"]["spaces_dir"]

db = client.handykapp


@flow
def load_rapid_horseracing_data():
    logger = get_run_logger()
    logger.info("Starting rapid_horseracing loader")

    r = record_processor()
    next(r)

    files = SpacesClient.get_files(f"{SOURCE}results")

    for file in files:
        if file != "results_to_do_list.json":
            data = SpacesClient.read_file(file)
            record = RapidRecord(**data)
            r.send((record, transform_results, file, "rapid"))

    r.close()


if __name__ == "__main__":
    load_rapid_horseracing_data()
