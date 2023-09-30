# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from transformers.bha_transformer import bha_transformer
from transformers.parsers import parse_horse
from transformers.core_transformer import core_transformer
from loaders.adders import add_horse, add_person, add_racecourse
from nameparser import HumanName  # type: ignore
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


def convert_parent(name, sex):
    return {"name": name, "sex": sex}


def convert_person(name, source):
    parsed_name = HumanName(name).as_dict()
    parsed_name["display_name"] = {source: name}
    return parsed_name


def convert_value_to_id(horse, value, lookup):
    if value:
        horse[value] = (
            lookup.get(parse_horse(horse[value]), None)
            if value in ["sire", "dam"]
            else lookup.get(horse[value], None)
        )
    return horse


@task(tags=["BHA"])
def load_horses(horses):
    logger = get_run_logger()
    ret_val = {}
    for i, horse in enumerate(horses):
        horse["name"], horse["country"] = parse_horse(horse["name"], "GB")
        ret_val[(horse["name"], horse.get("country"))] = add_horse(horse)

        if i % 250 == 0:
            logger.info(f"Loaded {i} horses")

    return ret_val


@task(tags=["BHA"])
def load_people(people, source):
    ret_val = {}
    for person in people:
        if person:
            ret_val[person] = add_person(convert_person(person, source))
    return ret_val


@task(tags=["Rapid"])
def load_races(races):
    for race in races:
        del race["result"]
        db.races.insert_one(race)


@task(tags=["Core"])
def load_racecourses(racecourses):
    for racecourse in racecourses:
        add_racecourse(racecourse)


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
def load_bha_horses():
    data = bha_transformer()
    sires = select_set(data, "sire")
    dams = select_set(data, "dam")
    trainers = select_set(data, "trainer")
    sires_ids = load_horses([convert_parent(x, "M") for x in sires])
    dams_ids = load_horses([convert_parent(x, "F") for x in dams])
    trainer_ids = load_people(trainers, "bha")
    data = [convert_value_to_id(x, "sire", sires_ids) for x in data]
    data = [convert_value_to_id(x, "dam", dams_ids) for x in data]
    data = [convert_value_to_id(x, "trainer", trainer_ids) for x in data]
    load_horses(data)


@flow
def load_database_afresh():
    # drop_database()
    # spec_database()
    # load_bha_horses()
    # races = rapid_horseracing_transformer()
    # load_races(races)
    racecourses = core_transformer()
    load_racecourses(racecourses)


if __name__ == "__main__":
    load_database_afresh()  # type: ignore
