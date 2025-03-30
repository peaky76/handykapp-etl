# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import tomllib
from prefect import flow, get_run_logger

from clients import mongo_client as client
from helpers import get_files, read_file
from models import TheRacingApiRacecard
from processors.record_processor import record_processor
from transformers.theracingapi_transformer import transform_races

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]

db = client.handykapp


@flow
def increment_theracingapi_data():
    logger = get_run_logger()
    logger.info("Querying database for most recent race")
    races = list(db.races.find().sort("datetime", -1))
    logger.info(f"{len(races)} races found")
    if races:
        most_recent = races[-1]["datetime"]
        logger.info(f"Most recent race on db is: {pendulum.parse(most_recent)}")
        load_theracingapi_data(from_date=pendulum.parse(most_recent))
    else:
        logger.info("No races currently in db")
        load_theracingapi_data()


@flow
def load_theracingapi_data(*, from_date=None):
    logger = get_run_logger()
    logger.info("Starting theracingapi loader")

    r = record_processor()
    next(r)
    for file in get_files(f"{SOURCE}racecards"):
        if from_date:
            file_date = pendulum.parse(file.split(".")[0][-8:])
            if file_date < from_date:
                continue

        logger.info(f"Reading {file}")
        contents = read_file(file)
        for dec in contents["racecards"]:
            data = {k: v for k, v in dec.items() if k != "off_dt"}
            record = TheRacingApiRacecard(**data)
            r.send((record.model_dump(), transform_races, file, "theracingapi"))

    r.close()


if __name__ == "__main__":
    increment_theracingapi_data()
