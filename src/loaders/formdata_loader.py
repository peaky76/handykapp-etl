# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


import tomllib
from prefect import flow, get_run_logger

from clients import mongo_client as client
from processors.formdata_processors import file_processor
from transformers.formdata_transformer import get_formdatas

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["formdata"]["spaces_dir"]

db = client.handykapp


@flow
def load_formdata_only():
    logger = get_run_logger()
    logger.info("Starting formdata loader")

    db.formdata.drop()
    logger.info("Dropped formdata collection")

    f = file_processor()
    next(f)

    files = get_formdatas(after_year=24, for_refresh=True)
    for file in files:
        f.send(file)

    f.close()
    logger.info("Loaded formdata collection")


if __name__ == "__main__":
    load_formdata_only()  # type: ignore
