# To allow running as a script
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import fitz  # type: ignore
import tomllib
from peak_utility.names.corrections import scotify  # type: ignore
from peak_utility.text.case import normal  # type: ignore
from prefect import flow, get_run_logger

from clients import mongo_client as client
from helpers import stream_file
from models.formdata_horse import FormdataHorse
from processors.horse_processor import horse_processor
from transformers.formdata_transformer import (
    create_horse,
    create_run,
    get_formdata_date,
    get_formdatas,
    is_horse,
    is_race_date,
)

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


def word_processor():
    logger = get_run_logger()
    logger.info("Starting word processor")
    source = "racing_research"
    horse = None
    horse_args = []
    run_args = []
    adding_horses = False
    adding_runs = False

    fl = formdata_loader()
    next(fl)
    hp = horse_processor()
    next(hp)

    try:
        while True:
            item = yield
            word, date = item
            if "FORMDATA" in word:
                skip_count = 3
                continue

            if skip_count > 0:
                skip_count -= 1
                continue

            horse_switch = is_horse(word)
            run_switch = is_race_date(word)

            # Switch on/off adding horses/runs
            if horse_switch:
                adding_horses = True
                adding_runs = False
            elif run_switch:
                adding_horses = False
                adding_runs = True
            elif "then" in word:
                adding_horses = False
                adding_runs = False

            # Create horses/runs
            if run_switch and len(horse_args):
                horse = create_horse(horse_args, date.year)
                horse_args = []

            if (horse_switch or run_switch) and len(run_args):
                run = create_run(run_args)
                if horse and run is None:
                    logger.error(f"Missing run for {horse.name}")
                elif horse:
                    horse.runs.append(run)
                else:
                    logger.error("Run created but no horse to add it to")
                run_args = []

            # Add horses/runs to db
            if horse_switch and horse:
                fl.send((horse, date))
                # hp.send((horse, source))
                horse = None

            # Add words to horses/runs
            if adding_horses:
                horse_args.append(word)
            elif adding_runs:
                run_args.append(word)

    except GeneratorExit:
        hp.close()


def page_processor():
    logger = get_run_logger()
    logger.info("Starting page processor")

    w = word_processor()
    next(w)

    try:
        while True:
            item = yield
            page, date = item
            text = page.get_text()
            # Replace non-ascii characters with apostrophes
            words = (
                text.replace(f"{chr(10)}{chr(25)}", "'")  # Newline + apostrophe
                .replace(f"{chr(32)}{chr(25)}", "'")  # Space + apostrophe
                .replace(chr(25), "'")  # Regular apostrophe
                .replace(chr(65533), "'")  # Replacement character
                .split("\n")
            )
            for word in words:
                w.send((word, date))

    except GeneratorExit:
        w.close()


def file_processor():
    logger = get_run_logger()
    logger.info("Starting file processor")
    page_count = 0

    p = page_processor()
    next(p)

    try:
        while True:
            file = yield
            logger.info(f"Processing {file}")

            date = get_formdata_date(file)
            doc = fitz.open("pdf", stream_file(file))
            for page in doc:
                p.send((page, date))
                page_count += 1

    except GeneratorExit:
        logger.info(f"Processed {page_count} pages")
        p.close()


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
