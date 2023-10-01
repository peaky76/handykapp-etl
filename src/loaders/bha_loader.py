# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from transformers.bha_transformer import bha_transformer
from transformers.parsers import parse_horse
from loaders.adders import add_horse, add_person
from nameparser import HumanName  # type: ignore
from prefect import flow, task, get_run_logger

import yaml

with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["spaces"]["dir"]


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


# @task(tags=["Rapid"])
# def load_races(races):
#     for race in races:
#         del race["result"]
#         db.races.insert_one(race)


@task(tags=["BHA"])
def select_set(data, key):
    return sorted(list(set([x[key] for x in data])))


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
def load_bha_afresh():
    pass
    # drop_database()
    # spec_database()
    # load_bha_horses()
    # races = rapid_horseracing_transformer()
    # load_races(races)


if __name__ == "__main__":
    load_database_afresh()  # type: ignore
