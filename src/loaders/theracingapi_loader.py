# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
from typing import Generator, Optional

import pendulum
import petl
import tomllib
from clients import mongo_client as client
from helpers import get_files, read_file, stream_file
from prefect import flow, get_run_logger
from processors.database_processors import DeclarationProcessor
from transformers.theracingapi_transformer import TheRacingApiTransformer

from .loader import Loader

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]

db = client.handykapp

def filter_files(* , from_date: pendulum.DateTime = None):
    files = get_files(f"{SOURCE}racecards")
    if from_date:
        files = [file for file in files if pendulum.parse(file.split(".")[0][-8:]) >= from_date]
    return files


# @flow
# def increment_theracingapi_data():
#     logger = get_run_logger()
#     logger.info("Querying database for most recent race")
#     races = list(db.races.find().sort("datetime", -1))
#     logger.info(f"{len(races)} races found")
#     if races:
#         most_recent = races[-1]["datetime"]
#         logger.info(f"Most recent race on db is: {pendulum.parse(most_recent)}")
#         load_theracingapi_data(from_date=pendulum.parse(most_recent))
#     else:
#         logger.info("No races currently in db")
#         load_theracingapi_data()


@flow
def load_theracingapi_data(*, from_date=None):
    for file in filter_files(from_date=from_date):
        contents = read_file(file)
        decs = petl.fromdicts([{k: v for k, v in dec.items() if k != "off_dt"} for dec in contents["racecards"]]) 
        data = TheRacingApiTransformer(decs).transform()
        loader = Loader(data, DeclarationProcessor())
        loader.load()


if __name__ == "__main__":
    load_theracingapi_data()
