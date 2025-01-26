# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger, task
from pymongo.errors import DuplicateKeyError

from clients import mongo_client as client
from loaders.shared import convert_person, convert_value_to_id, select_set
from transformers.bha_transformer import bha_transformer
from transformers.parsers import parse_horse

db = client.handykapp


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
            inserted_horse = db.horses.insert_one(horse)
            ret_val[horse["name"], horse.get("country")] = inserted_horse.inserted_id
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
                inserted_person = db.people.insert_one(convert_person(person, source))
                ret_val[person] = inserted_person.inserted_id
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
            logger.info(f"Added trainers to {count} horses")

    logger.info(f"Added trainers to {count} horses")


@flow
def enrich_with_bha_ratings(data=None, lookup={}):
    logger = get_run_logger()

    if data is None:
        data = bha_transformer()

    count = 0
    for horse in data:
        horse["name"], horse["country"] = parse_horse(horse["name"], "GB")
        db.horses.update_one(
            {"_id": lookup[horse["name"], horse["country"]]},
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


@flow
def load_bha(data=None):
    if data is None:
        data = bha_transformer()

    horse_lookup = load_bha_horses(data)
    person_lookup = load_bha_people(data)
    enrich_with_bha_ratings(data, horse_lookup)
    associate_horse_with_trainer(data, horse_lookup, person_lookup)


@flow
def load_bha_afresh(data=None):
    if data is None:
        data = bha_transformer()

    db.horses.drop()
    db.people.drop()
    load_bha(data)


if __name__ == "__main__":
    load_bha_afresh()
