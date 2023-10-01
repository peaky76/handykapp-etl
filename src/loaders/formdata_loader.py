# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from transformers.formdata_transformer import formdata_transformer
from prefect import flow, task
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


def create_code_to_course_dict():
    source = db.racecourses.find(
        projection={"_id": 2, "references": {"racing_research": 1}}
    )
    return {
        racecourse["references"]["racing_research"]: racecourse["_id"]
        for racecourse in source
    }


@task
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


@task
def load_horses(formdata):
    ret_val = {}
    for entry in formdata:
        entry = entry._asdict()
        del entry["runs"]
        del entry["prize_money"]
        del entry["trainer_form"]
        entry_id = db.horses.insert_one(entry)
        ret_val[f"{entry['name']} ({entry['country']})"] = entry_id.inserted_id
    return ret_val


@task
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


@task
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
    horse_ids = load_horses(formdata)
    load_races(formdata)
    load_runs(formdata, horse_ids)


if __name__ == "__main__":
    load_formdata_afresh()  # type: ignore
