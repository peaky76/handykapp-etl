# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import tomllib
from clients import mongo_client as client
from helpers import get_files
from nameparser import HumanName
from prefect import flow, get_run_logger
from pymongo.errors import DuplicateKeyError
from transformers.theracingapi_transformer import theracingapi_transformer

from loaders.adders import add_horse, add_person

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]

db = client.handykapp


def make_search_dictionary(horse):
    keys = ["name", "country", "year"] if horse.get("country") else ["name", "sex"]

    return {k: horse[k] for k in keys}


def make_update_dictionary(horse, lookup):
    update_dictionary = {}
    if colour := horse.get("colour"):
        update_dictionary["colour"] = colour
    if horse.get("sire"):
        update_dictionary["sire"] = lookup.get(horse["sire"])
    if horse.get("dam"):
        update_dictionary["dam"] = lookup.get(horse["dam"])
    return update_dictionary


def declaration_processor():
    logger = get_run_logger()
    logger.info("Starting declaration processor")
    racecourse_ids = {}
    # racecourse_adds_count = 0
    declaration_adds_count = 0
    declaration_skips_count = 0

    h = horse_processor()
    next(h)

    try:
        while True:
            dec = yield
            racecourse_id = racecourse_ids.get(
                (dec["course"], dec["surface"], dec["code"], dec["obstacle"])
            )

            if not racecourse_id:
                surface_options = (
                    ["Tapeta", "Polytrack"] if dec["surface"] == "AW" else ["Turf"]
                )
                racecourse = db.racecourses.find_one(
                    {
                        "name": dec["course"].title(),
                        "surface": {"$in": surface_options},
                        "code": dec["code"],
                        "obstacle": dec["obstacle"],
                    },
                    {"_id": 1},
                )

                if racecourse:
                    racecourse_id = racecourse["_id"]
                    racecourse_ids[
                        (dec["course"], dec["surface"], dec["code"], dec["obstacle"])
                    ] = racecourse_id

            if racecourse_id:
                try:
                    declaration = db.races.insert_one(
                        {
                            "racecourse": racecourse_id,
                            "datetime": dec["datetime"],
                            "title": dec["title"],
                            "is_handicap": dec["is_handicap"],
                            "distance_description": dec["distance_description"],
                            "race_grade": dec["race_grade"],
                            "race_class": dec["race_class"],
                            "age_restriction": dec["age_restriction"],
                            "rating_restriction": dec["rating_restriction"],
                            "prize": dec["prize"],
                        }
                    )
                    declaration_adds_count += 1

                    for horse in dec["runners"]:
                        h.send({"name": horse["sire"], "sex": "M", "race_id": None})
                        h.send({"name": horse["damsire"], "sex": "M", "race_id": None})
                        h.send(
                            {
                                "name": horse["dam"],
                                "sex": "F",
                                "sire": horse["damsire"],
                                "race_id": None,
                            }
                        )
                        h.send(horse | {"race_id": declaration.inserted_id})
                except DuplicateKeyError:
                    logger.warning(
                        f"Duplicate declaration for {dec['datetime']} at {dec['course']}"
                    )
            else:
                declaration_skips_count += 1

    except GeneratorExit:
        logger.info(
            f"Finished processing declarations. Added {declaration_adds_count} declarations, skipped {declaration_skips_count}"
        )
        h.close()


def horse_processor():
    logger = get_run_logger()
    logger.info("Starting horse processor")
    horse_ids = {}
    updated_count = 0
    adds_count = 0
    skips_count = 0

    p = person_processor()
    next(p)

    try:
        while True:
            horse = yield
            race_id = horse["race_id"]
            name = horse["name"]

            # Add horse to db if not already there
            if name in horse_ids:
                logger.debug(f"{name} skipped")
                skips_count += 1
            else:
                result = db.horses.find_one(make_search_dictionary(horse), {"_id": 1})

                if result:
                    db.horses.update_one(
                        {"_id": result["_id"]},
                        {"$set": make_update_dictionary(horse, horse_ids)},
                    )
                    logger.debug(f"{name} updated")
                    updated_count += 1
                    horse_ids[name] = result["_id"]
                else:
                    added_id = add_horse(
                        {
                            k: v
                            for k, v in {
                                "name": name,
                                "sex": horse["sex"],
                                "year": horse.get("year"),
                                "country": horse.get("country"),
                                "colour": horse.get("colour"),
                                "sire": horse_ids.get(horse["sire"])
                                if horse.get("sire")
                                else None,
                                "dam": horse_ids.get(horse["dam"])
                                if horse.get("dam")
                                else None,
                            }.items()
                            if v
                        }
                    )
                    logger.debug(f"{name} added to db")
                    adds_count += 1
                    horse_ids[name] = added_id

            # Add horse to race
            if race_id:
                db.races.update_one(
                    {"_id": race_id},
                    {
                        "$push": {
                            "runners": {
                                "horse": horse_ids[name],
                                "owner": horse["owner"],
                                "allowance": horse["allowance"],
                                "saddlecloth": horse["saddlecloth"],
                                "draw": horse["draw"],
                                "headgear": horse["headgear"],
                                "lbs_carried": horse["lbs_carried"],
                                "official_rating": horse["official_rating"],
                            }
                        }
                    },
                )
                p.send(
                    {
                        "name": horse["trainer"],
                        "role": "trainer",
                        "race_id": race_id,
                        "runner_id": horse_ids[name],
                    }
                )
                p.send(
                    {
                        "name": horse["jockey"],
                        "role": "jockey",
                        "race_id": race_id,
                        "runner_id": horse_ids[name],
                    }
                )

    except GeneratorExit:
        logger.info(
            f"Finished processing horses. Updated {updated_count}, added {adds_count}, skipped {skips_count}"
        )
        p.close()


def person_processor():
    logger = get_run_logger()
    logger.info("Starting person processor")
    person_ids = {}
    updated_count = 0
    adds_count = 0
    skips_count = 0

    try:
        while True:
            person = yield
            found_id = None
            name = person["name"]
            race_id = person["race_id"]
            runner_id = person["runner_id"]
            role = person["role"]

            # Add person to db if not there
            if name in person_ids:
                logger.debug(f"{person} skipped")
                skips_count += 1
            else:
                name_parts = HumanName(name)
                result = db.people.find({"last": name_parts.last})
                for possibility in result:
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
                        {"$set": {"references.theracingapi": name}},
                    )
                    logger.debug(f"{person} updated")
                    updated_count += 1
                else:
                    found_id = add_person(
                        name_parts.as_dict() | {"references.theracingapi": name}
                    )
                    logger.info(f"{person} added to db")
                    adds_count += 1

                person_ids[name] = found_id

            # Add person to horse in race
            if race_id:
                db.races.update_one(
                    {"_id": race_id, "runners.horse": runner_id},
                    {"$set": {f"runners.$.{role}": person_ids[name]}},
                )
                updated_count += 1

    except GeneratorExit:
        logger.info(
            f"Finished processing people. Updated {updated_count}, added {adds_count}, skipped {skips_count}"
        )


@flow
def load_theracingapi_data():
    logger = get_run_logger()
    logger.info("Starting theracingapi loader")

    t = theracingapi_transformer()
    next(t)
    for file in get_files(f"{SOURCE}racecards"): 
        t.send(file)

    t.close()


@flow
def load_theracingapi_data_old(data=None):
    logger = get_run_logger()
    if data is None:
        data = theracingapi_transformer()
    race_count = 0

    d = declaration_processor()
    next(d)
    for race in data:
        logger.debug(f"Processing {race['datetime']} from {race['course']}")
        d.send(race)
        race_count += 1

        if race_count and race_count % 100 == 0:
            logger.info(f"Processed {race_count} races")

    logger.info(f"Processed {race_count} races")
    d.close()


@flow
def load_theracingapi_data_afresh(data=None):
    if data is None:
        data = theracingapi_transformer()

    db.horses.drop()
    db.races.drop()
    db.people.drop()
    load_theracingapi_data(data)


if __name__ == "__main__":
    load_theracingapi_data()
