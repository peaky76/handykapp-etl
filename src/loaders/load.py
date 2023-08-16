# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from transformers.bha_transformer import bha_transformer
from transformers.parsers import parse_horse
from transformers.rapid_horseracing_transformer import rapid_horseracing_transformer
from nameparser import HumanName
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
    db.people.create_index(
        [("last", ASC), ("first", ASC), ("middle", ASC)], unique=True
    )
    db.racecourses.create_index([("name", ASC), ("country", ASC)], unique=True)
    db.races.create_index([("venue", ASC), ("datetime", ASC)], unique=True)
    db.races.create_index("result.horse")


def add_horse(horse):
    horse_id = db.horses.insert_one(horse)
    return horse_id.inserted_id


def add_person(person, source):
    name = HumanName(person)
    person_id = db.people.insert_one(
        {
            "title": name.title,
            "first": name.first,
            "middle": name.middle,
            "last": name.last,
            "suffix": name.suffix,
            "display_name": {source: person},
        }
    )
    return person_id.inserted_id


@task(tags=["BHA"])
def load_people(people, source):
    ret_val = {}
    for person in people:
        if person:
            ret_val[person] = add_person(person, source)
    return ret_val


@task(tags=["BHA"])
def load_parents(horses, sex):
    ret_val = {}
    for horse in horses:
        name, country = parse_horse(horse)
        ret_val[(name, country)] = add_horse({name: name, country: country, sex: sex})
    return ret_val


@task(tags=["BHA"])
def load_horse_detail(horses, sires_ids, dams_ids, trainer_ids):
    logger = get_run_logger()
    for i, horse in enumerate(horses):
        horse["sire"]: sires_ids.get((horse["sire"], horse["sire_country"]), None)
        horse["dam"]: dams_ids.get((horse["dam"], horse["dam_country"]), None)
        horse["trainer"]: trainer_ids.get(horse["trainer"], None)
        if horse["is_gelded"]:
            horse["operations"] = {"type": "gelding", "date": None}
        del horse["sire_country"]
        del horse["dam_country"]
        del horse["is_gelded"]

        add_horse(horse)

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


@task(tags=["BHA"])
def select_set(data, key):
    return sorted(list(set([x[key] for x in data])))


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
    sires = select_set(data, "sire")
    dams = select_set(data, "dam")
    trainers = select_set(data, "trainer")
    drop_database()
    spec_database()
    sires_ids = load_parents(sires, "M")
    dams_ids = load_parents(dams, "F")
    trainer_ids = load_people(trainers, "bha")
    load_horse_detail(data, sires_ids, dams_ids, trainer_ids)
    races = rapid_horseracing_transformer()
    load_races(races)


if __name__ == "__main__":
    load_database_afresh()  # type: ignore
