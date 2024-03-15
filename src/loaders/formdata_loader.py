# To allow running as a script
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import tomllib
from clients import mongo_client as client
from peak_utility.text.case import normal  # type: ignore
from prefect import flow, get_run_logger
from processors.file_processor import FileProcessor
from transformers.formdata_transformer import get_formdatas

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["formdata"]["spaces_dir"]

db = client.handykapp


def adjust_rr_name(name):
    country = name.split("(")[-1].replace(")", "") if "(" in name else None
    name = name.replace(" (" + country + ")", "") if country else name

    # TODO : Will be in next v0.5 of peak_utility
    scottishise = (
        lambda x: re.sub(r"(m(?:a*)c)(\s*)", r"\1 ", x, flags=re.IGNORECASE)
        .title()
        .replace("Mac ", "Mac")
        .replace("Mc ", "Mc")
    )

    name = scottishise(normal(name))
    name = re.sub(
        r"([a-z])'([A-Z])",
        lambda match: match.group(1) + "'" + match.group(2).lower(),
        name,
    )

    return f"{name} ({country})" if country else name

@flow
def load_formdata_only():
    logger = get_run_logger()

    db.formdata.drop()
    logger.info("Dropped formdata collection")

    f = FileProcessor()()
    next(f)

    files = get_formdatas(after_year=20, for_refresh=True)
    for file in files:
        f.send(file)

    f.close()
    logger.info("Loaded formdata collection")


if __name__ == "__main__":
    load_formdata_only()  # type: ignore
