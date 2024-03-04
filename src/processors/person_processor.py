from clients import mongo_client as client
from nameparser import HumanName  # type: ignore
from pymongo.errors import DuplicateKeyError

from .processor import Processor

db = client.handykapp


def person_updater(person, ratings, source):
    name_parts = HumanName(person["name"])

    possibilities = db.people.find({"last": name_parts.last})
    for possibility in possibilities:
        if name_parts.first == possibility["first"] or (
            name_parts.first
            and possibility["first"]
            and name_parts.first[0] == possibility["first"][0]
            and name_parts.title == possibility["title"]
        ):
            found_person = possibility
            break

    if found_person:
        person_id = found_person["_id"]
        update_data = (
            {f"references.{source}": person["name"]} | {"ratings": ratings}
            if ratings
            else {}
        )
        db.people.update_one(
            {"_id": person_id},
            {"$set": update_data},
        )
        return person_id

    return None


def person_inserter(person, ratings, source):
    name_parts = HumanName(person["name"])

    inserted_person = db.people.insert_one(
        name_parts.as_dict()
        | {f"references.{source}": person["name"]}
        | {"ratings": ratings}
        if ratings
        else {}
    )
    found_id = inserted_person.inserted_id

def person_processor_func(person, source, logger, next_processor):
    added_count = 0
    updated_count = 0
    skipped_count = 0

    ratings = {} # TODO: Get ratings as an additional element in yield
    name = person["name"]
    race_id = person.get("race_id")
    runner_id = person.get("runner_id")
    role = person.get("role")

    if (person_id := person_updater(person, ratings, source)):
        logger.debug(f"{person} updated")
        updated_count += 1
    else:
        try:
            person_id = person_inserter(person, ratings, source)
            logger.debug(f"{person} added to db")
            added_count += 1
        except DuplicateKeyError:
            logger.warning(f"Duplicate person: {name}")
            skipped_count += 1

    # Add person to horse in race
    if race_id:
        db.races.update_one(
            {"_id": race_id, "runners.horse": runner_id},
            {"$set": {f"runners.$.{role}": person_id}},
        )

    return added_count, updated_count, skipped_count

person_processor = Processor("person", person_processor_func).process
