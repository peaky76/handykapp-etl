# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger

# from loaders.adders import add_horse, add_person
from clients import mongo_client as client
from transformers.rapid_horseracing_transformer import rapid_horseracing_transformer

# from nameparser import HumanName
# from pymongo.errors import DuplicateKeyError

db = client.handykapp


# def make_search_dictionary(horse):
#     if horse.get("country"):
#         keys = ["name", "country", "year"]
#     else:
#         keys = ["name", "sex"]

#     return {k: horse[k] for k in keys}


# def make_update_dictionary(horse, lookup):
#     update_dictionary = {}
#     if colour := horse.get("colour"):
#         update_dictionary["colour"] = colour
#     if horse.get("sire"):
#         update_dictionary["sire"] = lookup.get(horse["sire"])
#     if horse.get("dam"):
#         update_dictionary["dam"] = lookup.get(horse["dam"])
#     return update_dictionary


def result_processor():
    logger = get_run_logger()
    logger.info("Starting result processor")
    racecourse_ids = {}
    race_added_count = 0
    race_updated_count = 0
    result_skips_count = 0

    #     h = horse_processor()
    #     next(h)

    try:
        while True:
            result = yield
            racecourse_id = racecourse_ids.get(
                (
                    result["course"],
                    result["surface"],
                    result["code"],
                    result["obstacle"],
                )
            )

            if not racecourse_id:
                surface_options = (
                    ["Tapeta", "Polytrack"] if result["surface"] == "AW" else ["Turf"]
                )
                racecourse = db.racecourses.find_one(
                    {
                        "name": result["course"].title(),
                        "surface": {"$in": surface_options},
                        "code": result["code"],
                        "obstacle": result["obstacle"],
                    },
                    {"_id": 1},
                )

                if racecourse:
                    racecourse_id = racecourse["_id"]
                    racecourse_ids[
                        (
                            result["course"],
                            result["surface"],
                            result["code"],
                            result["obstacle"],
                        )
                    ] = racecourse_id

            if racecourse_id:
                race = db.race.find_one(
                    {"racecourse": racecourse_id, "datetime": result["datetime"]}
                )

                # TODO: Check race matches rapid_horseracing data
                if race:
                    race_id = race["_id"]
                    db.race.update_one(
                        {"_id": race_id},
                        {
                            "$set": {
                                "rapid_id": result["rapid_id"],
                                "going_description": result["going_description"],
                            }
                        },
                    )
                    logger.debug(f"{result['datetime']} at {result['course']} updated")
                    race_updated_count += 1
                else:
                    race_id = db.race.insert_one(
                        {
                            "racecourse": racecourse_id,
                            "datetime": result["datetime"],
                            "title": result["title"],
                            "is_handicap": result["is_handicap"],
                            "distance_description": result["distance_description"],
                            "going_description": result["going_description"],
                            "race_class": result["class"],
                            "age_restriction": result["age_restriction"],
                            "prize": result["prize"],
                            "rapid_id": result["rapid_id"],
                        }
                    )
                    logger.info(
                        f"{result['datetime']} at {result['course']} added to db"
                    )
                    race_added_count += 1

    #                     for horse in result["runners"]:
    #                         h.send({"name": horse["sire"], "sex": "M", "race_id": None})
    #                         h.send({"name": horse["damsire"], "sex": "M", "race_id": None})
    #                         h.send(
    #                             {
    #                                 "name": horse["dam"],
    #                                 "sex": "F",
    #                                 "sire": horse["damsire"],
    #                                 "race_id": None,
    #                             }
    #                         )
    #                         h.send(horse | {"race_id": result.inserted_id})
    #                 except DuplicateKeyError:
    #                     logger.warning(
    #                         f"Duplicate result for {dec['datetime']} at {dec['course']}"
    #                     )
    #             else:
    #                 result_skips_count += 1

    except GeneratorExit:
        logger.info(
            f"Finished processing results. Updated {race_updated_count} race, added {race_added_count} results"
        )


#         h.close()


# def horse_processor():
#     logger = get_run_logger()
#     logger.info("Starting horse processor")
#     horse_ids = {}
#     updated_count = 0
#     adds_count = 0
#     skips_count = 0

#     p = person_processor()
#     next(p)

#     try:
#         while True:
#             horse = yield
#             race_id = horse["race_id"]
#             name = horse["name"]

#             # Add horse to db if not already there
#             if name in horse_ids.keys():
#                 logger.debug(f"{name} skipped")
#                 skips_count += 1
#             else:
#                 result = db.horses.find_one(make_search_dictionary(horse), {"_id": 1})

#                 if result:
#                     db.horses.update_one(
#                         {"_id": result["_id"]},
#                         {"$set": make_update_dictionary(horse, horse_ids)},
#                     )
#                     logger.debug(f"{name} updated")
#                     updated_count += 1
#                     horse_ids[name] = result["_id"]
#                 else:
#                     added_id = add_horse(
#                         {
#                             k: v
#                             for k, v in {
#                                 "name": name,
#                                 "sex": horse["sex"],
#                                 "year": horse.get("year"),
#                                 "country": horse.get("country"),
#                                 "colour": horse.get("colour"),
#                                 "sire": horse_ids.get(horse["sire"])
#                                 if horse.get("sire")
#                                 else None,
#                                 "dam": horse_ids.get(horse["dam"])
#                                 if horse.get("dam")
#                                 else None,
#                             }.items()
#                             if v
#                         }
#                     )
#                     logger.debug(f"{name} added to db")
#                     adds_count += 1
#                     horse_ids[name] = added_id

#             # Add horse to race
#             if race_id:
#                 db.races.update_one(
#                     {"_id": race_id},
#                     {
#                         "$push": {
#                             "runners": {
#                                 "horse": horse_ids[name],
#                                 "owner": horse["owner"],
#                                 "allowance": horse["allowance"],
#                                 "saddlecloth": horse["saddlecloth"],
#                                 "draw": horse["draw"],
#                                 "headgear": horse["headgear"],
#                                 "lbs_carried": horse["lbs_carried"],
#                                 "official_rating": horse["official_rating"],
#                             }
#                         }
#                     },
#                 )
#                 p.send(
#                     {
#                         "name": horse["trainer"],
#                         "role": "trainer",
#                         "race_id": race_id,
#                         "runner_id": horse_ids[name],
#                     }
#                 )
#                 p.send(
#                     {
#                         "name": horse["jockey"],
#                         "role": "jockey",
#                         "race_id": race_id,
#                         "runner_id": horse_ids[name],
#                     }
#                 )

#     except GeneratorExit:
#         logger.info(
#             f"Finished processing horses. Updated {updated_count}, added {adds_count}, skipped {skips_count}"
#         )
#         p.close()


# def person_processor():
#     logger = get_run_logger()
#     logger.info("Starting person processor")
#     person_ids = {}
#     updated_count = 0
#     adds_count = 0
#     skips_count = 0

#     try:
#         while True:
#             person = yield
#             found_id = None
#             name = person["name"]
#             race_id = person["race_id"]
#             runner_id = person["runner_id"]
#             role = person["role"]

#             # Add person to db if not there
#             if name in person_ids.keys():
#                 logger.debug(f"{person} skipped")
#                 skips_count += 1
#             else:
#                 name_parts = HumanName(name)
#                 result = db.people.find({"last": name_parts.last})
#                 for possibility in result:
#                     if name_parts.first == possibility["first"]:
#                         found_id = possibility["_id"]
#                         break
#                     elif (
#                         name_parts.first
#                         and possibility["first"]
#                         and name_parts.first[0] == possibility["first"][0]
#                         and name_parts.title == possibility["title"]
#                     ):
#                         found_id = possibility["_id"]
#                         break

#                 if found_id:
#                     db.people.update_one(
#                         {"_id": found_id},
#                         {"$set": {"references.rapid_horseracing": name}},
#                     )
#                     logger.debug(f"{person} updated")
#                     updated_count += 1
#                 else:
#                     found_id = add_person(
#                         name_parts.as_dict() | {"references.rapid_horseracing": name}
#                     )
#                     logger.info(f"{person} added to db")
#                     adds_count += 1

#                 person_ids[name] = found_id

#             # Add person to horse in race
#             if race_id:
#                 db.races.update_one(
#                     {"_id": race_id, "runners.horse": runner_id},
#                     {"$set": {f"runners.$.{role}": person_ids[name]}},
#                 )
#                 updated_count += 1

#     except GeneratorExit:
#         logger.info(
#             f"Finished processing people. Updated {updated_count}, added {adds_count}, skipped {skips_count}"
#         )


@flow
def load_rapid_horseracing_data(data=None):
    logger = get_run_logger()
    if data is None:
        data = rapid_horseracing_transformer()
    race_count = 0

    r = result_processor()
    next(r)
    for race in data:
        logger.debug(f"Processing {race['datetime']} from {race['course']}")
        r.send(race)
        race_count += 1

        if race_count and race_count % 100 == 0:
            logger.info(f"Processed {race_count} races")

    logger.info(f"Processed {race_count} races")
    r.close()


@flow
def load_rapid_horseracing_data_afresh(data=None):
    if data is None:
        data = rapid_horseracing_transformer()

    db.horses.drop()
    db.races.drop()
    db.people.drop()
    load_rapid_horseracing_data(data)


if __name__ == "__main__":
    load_rapid_horseracing_data()
