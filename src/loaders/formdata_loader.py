# To allow running as a script
import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import fitz  # type: ignore
import tomllib
from clients import mongo_client as client
from helpers import stream_file
from horsetalk import RacingCode
from peak_utility.text.case import normal  # type: ignore
from prefect import flow, get_run_logger, task
from pymongo.errors import DuplicateKeyError
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

RACE_KEYS = [
    "date",
    "type",
    "win_prize",
    "course",
    "distance",
    "going",
    "number_of_runners",
]

RUN_KEYS = [
    "position",
    "weight",
    "headgear",
    "allowance",
    "jockey",
    "beaten_distance",
    "time_rating",
    "form_rating",
]


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


def create_code_to_course_dict():
    source = db.racecourses.find(
        projection={"_id": 2, "references": {"racing_research": 1}}
    )
    return {
        racecourse["references"]["racing_research"]: racecourse["_id"]
        for racecourse in source
    }


@task(tags=["Racing Research"])
def load_races(formdata):
    codes_to_courses = create_code_to_course_dict()
    for entry in formdata:
        for run in entry.runs:
            run = run._asdict()
            race = {k: v for k, v in run.items() if k in RACE_KEYS}
            race["course"] = codes_to_courses[race["course"]]
            race["runs"] = []
            try:  # noqa: SIM105
                db.races.insert_one(race)
            except DuplicateKeyError:
                pass


@task(tags=["Racing Research"])
def load_runs(formdata, horse_ids):
    codes_to_courses = create_code_to_course_dict()
    for entry in formdata:
        horse_id = horse_ids[f"{entry.name} ({entry.country})"]
        for run in entry.runs:
            run = run._asdict()
            db.races.find_one_and_update(
                {
                    "date": run["date"],
                    "course": codes_to_courses[run["course"]],
                    "type": run["type"],
                    "number_of_runners": run["number_of_runners"],
                    "win_prize": run["win_prize"],
                },
                {
                    "$push": {
                        "runs": {
                            "horse": horse_id,
                            **{k: v for k, v in run.items() if k in RUN_KEYS},
                        }
                    }
                },
            )


@flow
def load_formdata_horses(formdata=None):
    logger = get_run_logger()

    if formdata is None:
        formdata = formdata_transformer()

    ret_val = {}
    upsert_count = 0
    added_count = 0
    for entry in formdata:
        entry = entry._asdict()
        del entry["runs"]
        del entry["trainer_form"]
        entry_id = db.horses.update_one(
            {
                "name": entry["name"],
                "country": entry["country"],
                "year": entry["year"],
            },
            {"$set": {"prize_money": entry["prize_money"]}},
            upsert=True,
        )
        upsert_count += int(bool(entry_id.matched_count > 0))
        added_count += int(bool(entry_id.matched_count == 0))

        ret_val[(entry["name"], entry["country"])] = entry_id.upserted_id

    logger.info(f"Upserted {upsert_count} horses from Formdata")
    logger.info(f"Added {added_count} horses from Formdata")

    return ret_val


@flow
def load_formdata_people(formdata=None):
    logger = get_run_logger()

    if formdata is None:
        formdata = formdata_transformer()

    # ret_val = {}

    all_jockeys = []
    all_trainers = []

    for entry in formdata:
        entry = entry._asdict()
        all_trainers.append(entry["trainer"])
        jockeys = list({run._asdict()["jockey"] for run in entry["runs"]})
        all_jockeys.extend(jockeys)

    all_jockeys = list(set(all_jockeys))
    all_trainers = list(set(all_trainers))

    logger.info(f"Found {len(all_jockeys)} jockeys")
    logger.info(f"Found {len(all_trainers)} trainers")

    return {"jockeys": all_jockeys, "trainers": all_trainers}


###############################################


def formdata_loader():
    logger = get_run_logger()
    logger.info("Starting formdata loader")

    try:
        while True:
            item = yield
            horse, date = item

            entry = horse._asdict()
            entry["runs"] = [run._asdict() for run in entry["runs"]]

            existing_entry = db.formdata.find_one(
                {
                    "name": entry["name"],
                    "country": entry["country"],
                    "year": entry["year"],
                }
            )

            if existing_entry:
                runs = existing_entry["runs"]

                for new_run in entry["runs"]:
                    matched_run = next(
                        (r for r in runs if r["date"] == new_run["date"]),
                        None,
                    )
                    if matched_run:
                        runs.remove(matched_run)
                    runs.append(new_run)

                db.formdata.find_one_and_update(
                    {
                        "name": entry["name"],
                        "country": entry["country"],
                        "year": entry["year"],
                    },
                    {"$set": {"runs": runs}},
                )
            else:
                db.formdata.insert_one(entry)

    except GeneratorExit:
        pass


def horse_loader():
    try:
        while True:
            item = yield
            horse, date = item

    except GeneratorExit:
        pass


def word_processor():
    logger = get_run_logger()
    logger.info("Starting word processor")
    horse = None
    horse_args = []
    run_args = []
    adding_horses = False
    adding_runs = False

    fl = formdata_loader()
    next(fl)
    hl = horse_loader()
    next(hl)

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
                if run is None:
                    logger.error(f"Missing run for {horse.name}")
                else:
                    horse.runs.append(run)
                run_args = []

            # Add horses/runs to db
            if horse_switch and horse:
                fl.send((horse, date))
                hl.send((horse, date))
                horse = None

            # Add words to horses/runs
            if adding_horses:
                horse_args.append(word)
            elif adding_runs:
                run_args.append(word)

    except GeneratorExit:
        hl.close()


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


###############################################


@flow
def load_formdata_only():
    logger = get_run_logger()
    logger.info("Starting formdata loader")

    db.formdata.drop()
    logger.info("Dropped formdata collection")

    f = file_processor()
    next(f)

    files = get_formdatas(code=RacingCode.FLAT, after_year=20, for_refresh=True)
    for file in files:
        f.send(file)

    f.close()
    logger.info("Loaded formdata collection")


# @flow
# def load_formdata_afresh():
#     db.formdata.drop()
#     db.horses.drop()
#     db.races.create_index(
#         [
#             ("date", ASC),
#             ("course", ASC),
#             ("type", ASC),
#             ("number_of_runners", ASC),
#             ("win_prize", ASC),
#         ],
#         unique=True,
#     )
#     formdata = formdata_transformer()
#     load_formdata(formdata)
#     horse_ids = load_formdata_horses(formdata)
#     load_races(formdata)
#     load_runs(formdata, horse_ids)


if __name__ == "__main__":
    # data = load_formdata_people()  # type: ignore
    # print(data["jockeys"])
    # print(data["trainers"])

    # print([adjust_rr_name(x) for x in data["jockeys"]])
    # print([adjust_rr_name(x) for x in data["trainers"]])
    load_formdata_only()  # type: ignore
