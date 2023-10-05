# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, task, get_run_logger
from transformers.theracingapi_transformer import theracingapi_transformer
from clients import mongo_client as client

import yaml

with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["spaces"]["dir"]

db = client.handykapp


def horse_processor():
    logger = get_run_logger()
    logger.info("Starting horse processor")
    while True:
        horse = yield
        if horse:
            logger.info(f"Processing {horse['name']}")
        else:
            logger.info("Finished processing horses")
            break


def race_processor():
    logger = get_run_logger()
    logger.info("Starting race processor")

    h = horse_processor()
    next(h)
    while True:
        race = yield
        if race:
            logger.info(
                f"Processing {race['time']} from {race['course']} on {race['date']}"
            )
            for horse in race["runners"]:
                h.send(horse)
        else:
            logger.info("Finished processing races")
            break


@flow
def load_theracingapi_data_afresh(data=None):
    if data is None:
        data = theracingapi_transformer()

    r = race_processor()
    next(r)
    for race in data:
        r.send(race)


if __name__ == "__main__":
    load_theracingapi_data_afresh()
