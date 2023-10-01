# To allow running as a script
from pathlib import Path
import sys

from loaders.selectors import select_set

sys.path.append(str(Path(__file__).resolve().parent.parent))

from nameparser import HumanName  # type: ignore
from prefect import flow, task, get_run_logger
from pymongo.errors import DuplicateKeyError
from clients import mongo_client as client
from loaders.adders import add_horse, add_person
from transformers.bha_transformer import bha_transformer
from transformers.parsers import parse_horse

import yaml

with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["spaces"]["dir"]

db = client.handykapp


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


@task(tags=["BHA"], task_run_name="load_{descriptor}")
def load_horses(horses, descriptor="horses"):
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
            logger.info(f"Loaded {count} {descriptor}")

    logger.info(f"Loaded {count} {descriptor}")
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


@flow
def associate_horse_with_trainer(data=None, horse_lookup={}, person_lookup={}):
    logger = get_run_logger()

    if data is None:
        data = bha_transformer()

    count = 0
    for horse in data:
        if not horse.get("trainer"):
            continue

        horse["name"], horse["country"] = parse_horse(horse["name"], "GB")
        horse_id = horse_lookup.get((horse["name"], horse["country"]))
        if not horse_id:
            logger.warning(f"{horse['name']} ({horse['country']}) not in lookup table")
            continue

        convert_value_to_id(horse, "trainer", person_lookup)

        db.horses.update_one(
            {"_id": horse_id},
            {"$set": {"current_trainer": horse["trainer"]}},
            upsert=False,
        )
        count += 1

        if count and count % 250 == 0:
            logger.info(f"Added {count} trainers to horses")

    logger.info(f"Added {count} trainers to horses")


@flow
def enrich_with_bha_ratings(data=None, lookup={}):
    logger = get_run_logger()

    if data is None:
        data = bha_transformer()

    count = 0
    for horse in data:
        horse["name"], horse["country"] = parse_horse(horse["name"], "GB")
        db.horses.update_one(
            {"_id": lookup[(horse["name"], horse["country"])]},
            {"$set": {"ratings": horse["ratings"]}},
            upsert=False,
        )
        count += 1

        if count and count % 250 == 0:
            logger.info(f"Enriched {count} horses with ratings")

    logger.info(f"Enriched {count} horses with ratings")


@flow
def load_bha_horses(data=None):
    if data is None:
        data = bha_transformer()

    sires = select_set(data, "sire")
    dams = select_set(data, "dam")
    sires_ids = load_horses([{"name": name, "sex": "M"} for name in sires], "sires")
    dams_ids = load_horses([{"name": name, "sex": "F"} for name in dams], "dams")
    data = [convert_value_to_id(x, "sire", sires_ids) for x in data]
    data = [convert_value_to_id(x, "dam", dams_ids) for x in data]
    return load_horses(data)


@flow
def load_bha_people(data=None):
    if data is None:
        data = bha_transformer()

    trainers = select_set(data, "trainer")
    return load_people(trainers, "bha")


if __name__ == "__main__":
    data = bha_transformer()
    db.horses.drop()
    db.people.drop()
    horse_lookup = load_bha_horses(data)
    person_lookup = load_bha_people(data)
    enrich_with_bha_ratings(data, horse_lookup)
    associate_horse_with_trainer(data, horse_lookup, person_lookup)
