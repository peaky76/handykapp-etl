from clients import mongo_client as client
from nameparser import HumanName
from prefect import get_run_logger

db = client.handykapp


def person_processor():
    logger = get_run_logger()
    logger.info("Starting person processor")
    updated_count = 0
    adds_count = 0

    try:
        while True:
            person, source = yield
            found_id = None
            name = person["name"]
            race_id = person["race_id"]
            runner_id = person["runner_id"]
            role = person["role"]

            name_parts = HumanName(name)

            possibilities = db.people.find({"last": name_parts.last})
            for possibility in possibilities:
                if name_parts.first == possibility["first"] or (
                    name_parts.first
                    and possibility["first"]
                    and name_parts.first[0] == possibility["first"][0]
                    and name_parts.title == possibility["title"]
                ):
                    found_id = possibility["_id"]
                    break

            if found_id:
                db.people.update_one(
                    {"_id": found_id},
                    {"$set": {f"references.{source}": name}},
                )
                logger.debug(f"{person} updated")
                updated_count += 1
            else:
                inserted_person = db.people.insert_one(
                    name_parts.as_dict() | {f"references.#{source}": name}
                )
                found_id = inserted_person.inserted_id
                logger.info(f"{person} added to db")
                adds_count += 1

            # Add person to horse in race
            if race_id:
                db.races.update_one(
                    {"_id": race_id, "runners.horse": runner_id},
                    {"$set": {f"runners.$.{role}": found_id}},
                )

    except GeneratorExit:
        logger.info(
            f"Finished processing people. Updated {updated_count} and added {adds_count}"
        )
