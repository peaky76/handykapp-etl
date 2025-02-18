# To allow running as a script
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


import tomllib
from peak_utility.names.corrections import scotify  # type: ignore
from peak_utility.text.case import normal  # type: ignore
from prefect import flow, get_run_logger

from clients import mongo_client as client
from models.formdata_horse import FormdataHorse
from processors.formdata_processors import file_processor
from transformers.formdata_transformer import get_formdatas

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["formdata"]["spaces_dir"]

db = client.handykapp


def adjust_rr_name(name):
    country = name.split("(")[-1].replace(")", "") if "(" in name else None
    name = name.replace(" (" + country + ")", "") if country else name
    name = scotify(normal(name))
    name = re.sub(
        r"([a-z])'([A-Z])",
        lambda match: match.group(1) + "'" + match.group(2).lower(),
        name,
    )

    return f"{name} ({country})" if country else name


def create_code_to_course_dict():
    source = db.racecourses.find(
        projection={"_id": 2, "references": {"racing_research": 1}}
    )
    return {
        racecourse["references"]["racing_research"]: racecourse["_id"]
        for racecourse in source
    }


def formdata_loader():
    logger = get_run_logger()
    logger.info("Starting formdata loader")

    try:
        while True:
            item = yield
            horse, date = item

            existing_entry = db.formdata.find_one(
                {
                    "name": horse.name,
                    "country": horse.country,
                    "year": horse.year,
                }
            )

            if existing_entry:
                existing_horse = FormdataHorse.model_validate(existing_entry)
                runs = existing_horse.runs

                for new_run in horse.runs:
                    matched_run = next(
                        (r for r in runs if r.date == new_run.date),
                        None,
                    )
                    if matched_run:
                        runs.remove(matched_run)
                    runs.append(new_run)

                db.formdata.find_one_and_update(
                    {
                        "name": horse.name,
                        "country": horse.country,
                        "year": horse.year,
                    },
                    {
                        "$set": {
                            "runs": [run.model_dump() for run in runs],
                            "prize_money": horse.prize_money,
                            "trainer": horse.trainer,
                            "trainer_form": horse.trainer_form,
                        }
                    },
                )
            else:
                db.formdata.insert_one(horse.model_dump())

    except GeneratorExit:
        pass


@flow
def load_formdata_only():
    logger = get_run_logger()
    logger.info("Starting formdata loader")

    db.formdata.drop()
    logger.info("Dropped formdata collection")

    f = file_processor()
    next(f)

    files = get_formdatas(after_year=20, for_refresh=True)
    for file in files:
        f.send(file)

    f.close()
    logger.info("Loaded formdata collection")


if __name__ == "__main__":
    load_formdata_only()  # type: ignore
