# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from transformers.bha_transformer import bha_transformer
from transformers.parsers import parse_horse
from loaders.adders import add_horse, add_person
from nameparser import HumanName  # type: ignore
from prefect import flow, task, get_run_logger
from pymongo.errors import DuplicateKeyError

import yaml

with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["spaces"]["dir"]


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


def select_set(data, key):
    return sorted(list(set([x[key] for x in data])))


@task(tags=["BHA"])
def load_horses(horses):
    logger = get_run_logger()
    ret_val = {}
    count = 0

    for horse in horses:
        if horse.get("trainer") or horse.get("trainer") == "":
            del horse["trainer"]
        if horse.get("ratings"):
            del horse["ratings"]

        horse["name"], horse["country"] = parse_horse(horse["name"], "GB")

        try:
            ret_val[(horse["name"], horse.get("country"))] = add_horse(horse)
            count += 1
        except DuplicateKeyError:
            logger.warning(f"{horse['name']} ({horse['country']}) already in database")

        if count and count % 250 == 0:
            logger.info(f"Loaded {count} horses")

    logger.info(f"Loaded {count} horses")
    return ret_val


@task(tags=["BHA"])
def load_people(people, source):
    logger = get_run_logger()
    ret_val = {}
    count = 0

    for person in people:
        if person:
            try:
                ret_val[person] = add_person(convert_person(person, source))
                count += 1
            except DuplicateKeyError:
                logger.warning(f"{person} already in database")

        if count and count % 100 == 0:
            logger.info(f"Loaded {count} people")

    logger.info(f"Loaded {count} people")
    return ret_val


# @task(tags=["Rapid"])
# def load_races(races):
#     for race in races:
#         del race["result"]
#         db.races.insert_one(race)


@flow
def load_bha_horses():
    data = bha_transformer()
    sires = select_set(data, "sire")
    dams = select_set(data, "dam")
    # trainers = select_set(data, "trainer")
    sires_ids = load_horses([{"name": name, "sex": "M"} for name in sires])
    dams_ids = load_horses([{"name": name, "sex": "F"} for name in dams])
    # trainer_ids = load_people(trainers, "bha")
    data = [convert_value_to_id(x, "sire", sires_ids) for x in data]
    data = [convert_value_to_id(x, "dam", dams_ids) for x in data]
    # data = [convert_value_to_id(x, "trainer", trainer_ids) for x in data]
    load_horses(data)


@flow
def load_bha_people():
    data = bha_transformer()
    trainers = select_set(data, "trainer")
    load_people(trainers, "bha")


if __name__ == "__main__":
    # load_bha_horses()  # type: ignore
    load_bha_people()
