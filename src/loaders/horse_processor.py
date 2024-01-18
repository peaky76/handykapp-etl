
from functools import cache

from clients import mongo_client as client
from prefect import get_run_logger

from loaders.person_processor import person_processor

db = client.handykapp

@cache
def get_dam_id(horse):
    return db.horses.find_one({"name": horse, "sex": "F"}, {"_id": 1})["_id"]


@cache
def get_sire_id(horse):
    return db.horses.find_one({"name": horse, "sex": "M"}, {"_id": 1})["_id"]


def make_search_dictionary(horse):
    keys = ["name", "country", "year"] if horse.get("country") else ["name", "sex"]

    return {k: horse[k] for k in keys}


def make_update_dictionary(horse):
    update_dictionary = {}
    if colour := horse.get("colour"):
        update_dictionary["colour"] = colour
    if horse.get("sire"):
        update_dictionary["sire"] = get_sire_id(horse["sire"])
    if horse.get("dam"):
        update_dictionary["dam"] = get_dam_id(horse["dam"])
    return update_dictionary


def horse_processor():
    logger = get_run_logger()
    logger.info("Starting horse processor")
    updated_count = 0
    adds_count = 0

    p = person_processor()
    next(p)

    try:
        while True:
            horse, source = yield
            race_id = horse["race_id"]
            name = horse["name"]

            horse_id = db.horses.find_one(make_search_dictionary(horse), {"_id": 1})[
                "_id"
            ]

            if horse_id:
                db.horses.update_one(
                    {"_id": horse_id},
                    {"$set": make_update_dictionary(horse)},
                )
                logger.debug(f"{name} updated")
                updated_count += 1
            else:
                horse_id = db.horses.insert_one({
                    k: v
                    for k, v in {
                        "name": name,
                        "sex": horse["sex"],
                        "year": horse.get("year"),
                        "country": horse.get("country"),
                        "colour": horse.get("colour"),
                        "sire": get_sire_id(horse["sire"])
                        if horse.get("sire")
                        else None,
                        "dam": get_dam_id(horse["dam"]) if horse.get("dam") else None,
                    }.items()
                    if v
                })["inserted_id"]
                logger.debug(f"{name} added to db")
                adds_count += 1

            # Add horse to race
            if race_id:
                db.races.update_one(
                    {"_id": race_id},
                    {
                        "$push": {
                            "runners": {
                                "horse": horse_id,
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
                p.send({
                    "name": horse["trainer"],
                    "role": "trainer",
                    "race_id": race_id,
                    "runner_id": horse_id,
                }, source)
                p.send({
                    "name": horse["jockey"],
                    "role": "jockey",
                    "race_id": race_id,
                    "runner_id": horse_id,
                }, source)

    except GeneratorExit:
        logger.info(
            f"Finished processing horses. Updated {updated_count} and added {adds_count}"
        )
        p.close()

