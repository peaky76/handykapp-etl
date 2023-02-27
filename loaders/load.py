# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from helpers import stream_file
from transformers.bha_transformer import (
    get_file,
    select_dams,
    select_sires,
    parse_horse,
    parse_sex,
    transform_ratings_csv,
)
from prefect import flow, task, get_run_logger
import petl
import yaml

with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["spaces"]["dir"]

db = client.handykapp


@task
def drop_database():
    client.drop_database("handykapp")


@task(tags=["BHA"], task_run_name="load_basics")
def load_parents(horses, sex):
    return_val = {}
    for horse in horses:
        name, country = parse_horse(horse)
        id = db.horses.insert_one({"name": name, "country": country, "sex": sex})
        return_val[horse] = id.inserted_id
    return return_val


@task(tags=["BHA"], task_run_name="load_details")
def load_horse_detail(horses, sires_ids, dams_ids):
    logger = get_run_logger()
    for i, horse in enumerate(horses):
        name, country = parse_horse(horse["name"])
        sex = parse_sex(horse["sex"])
        db.horses.insert_one(
            {
                "name": name,
                "country": country,
                "sex": sex,
                "year": horse["year"],
                "sire": sires_ids.get(horse["sire"], None),
                "dam": dams_ids.get(horse["dam"], None),
                "trainer": horse["trainer"],
                "ratings": {
                    "flat": horse["flat"],
                    "aw": horse["aw"],
                    "chase": horse["chase"],
                    "hurdle": horse["hurdle"],
                },
            }
        )
        if i % 250 == 0:
            logger.info(f"Loaded {i} horses")


@task
def load_ratings():
    pass  # TODO


@flow
def create_sample_database():
    frankel = {
        "name": "Frankel",
        "country": "GB",
        "yob": 2008,
    }

    drop_database()
    db.horses.insert_one(frankel)


@flow
def load_database_afresh():
    source = petl.MemorySource(stream_file(get_file()))
    data = transform_ratings_csv(source)
    sires = select_sires(data)
    dams = select_dams(data)
    drop_database()
    sires_ids = load_parents(sires, "M")
    dams_ids = load_parents(dams, "F")
    load_horse_detail(data, sires_ids, dams_ids)
    # load_ratings()


if __name__ == "__main__":
    load_database_afresh()
