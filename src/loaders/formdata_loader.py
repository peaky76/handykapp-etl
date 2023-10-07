# To allow running as a script
from pathlib import Path
import re
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from peak_utility.text.case import normal  # type: ignore
from transformers.formdata_transformer import formdata_transformer
from prefect import flow, get_run_logger, task
from pymongo import ASCENDING as ASC
from pymongo.errors import DuplicateKeyError

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
def load_formdata(formdata):
    ret_val = {}
    for entry in formdata:
        run_dict = []
        for run in entry.runs:
            run_dict.append(run._asdict())
        entry = entry._asdict()
        entry["runs"] = run_dict

        entry_id = db.formdata.insert_one(entry)
        ret_val[f"{entry['name']} ({entry['country']})"] = entry_id.inserted_id
    return ret_val


@task(tags=["Racing Research"])
def load_races(formdata):
    codes_to_courses = create_code_to_course_dict()
    for entry in formdata:
        for run in entry.runs:
            run = run._asdict()
            race = {k: v for k, v in run.items() if k in RACE_KEYS}
            race["course"] = codes_to_courses[race["course"]]
            race["runs"] = []
            try:
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


@flow(tags=["Racing Research"])
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


@flow(tags=["Racing Research"])
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
        jockeys = list(set([run._asdict()["jockey"] for run in entry["runs"]]))
        all_jockeys.extend(jockeys)

    all_jockeys = [x for x in list(set(all_jockeys))]
    all_trainers = [x for x in list(set(all_trainers))]

    logger.info(f"Found {len(all_jockeys)} jockeys")
    logger.info(f"Found {len(all_trainers)} trainers")

    return {"jockeys": all_jockeys, "trainers": all_trainers}


@flow(tags=["Racing Research"])
def load_formdata_afresh():
    db.formdata.drop()
    db.horses.drop()
    db.races.create_index(
        [
            ("date", ASC),
            ("course", ASC),
            ("type", ASC),
            ("number_of_runners", ASC),
            ("win_prize", ASC),
        ],
        unique=True,
    )
    formdata = formdata_transformer()
    load_formdata(formdata)
    horse_ids = load_formdata_horses(formdata)
    load_races(formdata)
    load_runs(formdata, horse_ids)


if __name__ == "__main__":
    data = load_formdata_people()  # type: ignore
    # print(data["jockeys"])
    # print(data["trainers"])

    print([adjust_rr_name(x) for x in data["jockeys"]])
    print([adjust_rr_name(x) for x in data["trainers"]])
