# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from transformers.bha_transformer import bha_transformer
from transformers.rapid_horseracing_transformer import rapid_horseracing_transformer
from prefect import flow, task, get_run_logger
from pymongo import ASCENDING as ASC
import yaml

with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["spaces"]["dir"]

db = client.handykapp


@task
def drop_database():
    client.drop_database("handykapp")


@task
def spec_database():
    db.horses.create_index(
        [("name", ASC), ("country", ASC), ("year", ASC)], unique=True
    )
    db.horses.create_index("name")
    db.people.create_index("name", unique=True)
    db.racecourses.create_index([("name", ASC), ("country", ASC)], unique=True)
    db.races.create_index([("racecourse", ASC), ("datetime", ASC)], unique=True)
    db.races.create_index("result.horse")


@task(tags=["BHA"])
def load_people(people):
    ret_val = {}
    for person in people:
        id = db.people.insert_one({"name": person})
        ret_val[person] = id.inserted_id
    return ret_val


@task(tags=["BHA"])
def load_parents(horses, sex):
    ret_val = {}
    for horse in horses:
        id = db.horses.insert_one(
            {"name": horse["name"], "country": horse["country"], "sex": sex}
        )
        ret_val[(horse["name"], horse["country"])] = id.inserted_id
    return ret_val


@task(tags=["BHA"])
def load_horse_detail(horses, sires_ids, dams_ids, trainer_ids):
    logger = get_run_logger()
    for i, horse in enumerate(horses):
        data = {
            "name": horse["name"],
            "country": horse["country"],
            "sex": horse["sex"],
            "year": horse["year"],
            "sire": sires_ids.get((horse["sire"], horse["sire_country"]), None),
            "dam": dams_ids.get((horse["dam"], horse["dam_country"]), None),
            "trainer": trainer_ids.get(horse["trainer"], None),
            "ratings": {
                "flat": horse["flat"],
                "aw": horse["aw"],
                "chase": horse["chase"],
                "hurdle": horse["hurdle"],
            },
        }
        if horse["is_gelded"]:
            data["operations"] = {"type": "gelding", "date": None}
        db.horses.insert_one(data)
        if i % 250 == 0:
            logger.info(f"Loaded {i} horses")


@task(tags=["Rapid"])
def load_races(races):
    for race in races:
        del race["result"]
        db.races.insert_one(race)


@task
def load_ratings():
    pass  # TODO


def select_parent(data, parent):
    return [
        {"name": p[0], "country": p[1]}
        for p in sorted(
            list(
                set(
                    [(horse[f"{parent}"], horse[f"{parent}_country"]) for horse in data]
                )
            )
        )
    ]


@task(tags=["BHA"])
def select_dams(data):
    return select_parent(data, "dam")


@task(tags=["BHA"])
def select_sires(data):
    return select_parent(data, "sire")


@task(tags=["BHA"])
def select_trainers(data):
    return sorted(list(set([x["trainer"] for x in data])))


@flow
def create_sample_database():
    frankel = {
        "name": "Frankel",
        "country": "GB",
        "yob": 2008,
    }

    drop_database()
    spec_database()
    db.horses.insert_one(frankel)


@flow
def load_database_afresh():
    data = bha_transformer()
    sires = select_sires(data)
    dams = select_dams(data)
    trainers = select_trainers(data)
    drop_database()
    spec_database()
    sires_ids = load_parents(sires, "M")
    dams_ids = load_parents(dams, "F")
    trainer_ids = load_people(trainers)
    load_horse_detail(data, sires_ids, dams_ids, trainer_ids)
    load_races(rapid_horseracing_transformer())
    # load_ratings()


if __name__ == "__main__":
    load_database_afresh()  # type: ignore
