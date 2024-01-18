# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


import tomllib
from clients import mongo_client as client
from helpers import get_files, read_file
from prefect import flow, get_run_logger
from transformers.theracingapi_transformer import transform_races, validate_races

from loaders.dicts_processor import dicts_processor

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]

db = client.handykapp


@flow
def load_theracingapi_data():
    logger = get_run_logger()
    logger.info("Starting theracingapi loader")

    d = dicts_processor()
    next(d)
    for file in get_files(f"{SOURCE}racecards"):
        contents = read_file(file)
        filtered_contents = [
            {k: v for k, v in dec.items() if k != "off_dt"}
            for dec in contents["racecards"]
        ]
        d.send(filtered_contents, validate_races, transform_races, file, "theracingapi")

    d.close()


if __name__ == "__main__":
    load_theracingapi_data()
