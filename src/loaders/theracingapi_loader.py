# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger
from loaders.adders import add_horse
from transformers.theracingapi_transformer import theracingapi_transformer
from clients import mongo_client as client

db = client.handykapp


def make_search_dictionary(horse):
    if horse.get("country"):
        keys = ["name", "country", "year"]
    else:
        keys = ["name", "sex"]

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
    racecourse_adds_count = 0
    declaration_adds_count = 0

    try:
        while True:
            dec = yield
            racecourse_id = racecourse_ids.get(
                (dec["course"], dec["surface"], dec["obstacle"])
            )

            if not racecourse_id:
                racecourse = db.racecourses.find_one(
                    {
                        "name": dec["course"].title(),
                        "surface": dec["surface"].lower(),
                        "obstacle": dec["obstacle"],
                    },
                    {"_id": 1},
                )

                if racecourse:
                    racecourse_id = racecourse["_id"]
                    racecourse_ids[
                        (dec["course"], dec["surface"], dec["obstacle"])
                    ] = racecourse_id

            if racecourse_id:
                db.races.insert_one(
                    {
                        "racecourse": racecourse_id,
                        "date": dec["date"],
                        "time": dec["time"],
                        "title": dec["title"],
                        "is_handicap": dec["is_handicap"],
                        "distance_description": dec["distance_description"],
                        "grade": dec["grade"],
                        "class": dec["class"],
                        "age_restriction": dec["age_restriction"],
                        "rating_restriction": dec["rating_restriction"],
                        "prize": dec["prize"],
                    }
                )
                declaration_adds_count += 1

    except GeneratorExit:
        logger.info(
            f"Finished processing declarations. Added {racecourse_adds_count} courses, {declaration_adds_count} declarations"
        )


def horse_processor():
    logger = get_run_logger()
    logger.info("Starting horse processor")
    horse_ids = {}
    updated_count = 0
    adds_count = 0
    skips_count = 0

    try:
        while True:
            horse = yield
            name = horse["name"]

            if name in horse_ids.keys():
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
                    added_id = add_horse(horse)
                    logger.debug(f"{name} added to db")
                    adds_count += 1
                    horse_ids[name] = added_id

    except GeneratorExit:
        logger.info(
            f"Finished processing horses. Updated {updated_count}, added {adds_count}, skipped {skips_count}"
        )


def race_processor():
    logger = get_run_logger()
    logger.info("Starting race processor")
    race_count = 0

    d = declaration_processor()
    next(d)

    # h = horse_processor()
    # next(h)

    try:
        while True:
            race = yield
            if race:
                logger.debug(
                    f"Processing {race['time']} from {race['course']} on {race['date']}"
                )

                d.send(race)

                # for horse in race["runners"]:
                #     h.send({"name": horse["sire"], "sex": "M"})
                #     h.send({"name": horse["damsire"], "sex": "M"})
                #     h.send({"name": horse["dam"], "sex": "F", "sire": horse["damsire"]})
                #     h.send(horse)

                race_count += 1

            if race_count and race_count % 100 == 0:
                logger.info(f"Processed {race_count} races")

    except GeneratorExit:
        logger.info(f"Processed {race_count} races")
        d.close()


@flow
def load_theracingapi_data(data=None):
    if data is None:
        data = theracingapi_transformer()

    r = race_processor()
    next(r)
    for race in data:
        r.send(race)

    r.close()


if __name__ == "__main__":
    db.horses.drop()
    db.races.drop()
    load_theracingapi_data()
