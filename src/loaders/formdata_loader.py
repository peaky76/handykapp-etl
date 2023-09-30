# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from transformers.formdata_transformer import formdata_transformer
from prefect import flow, task
from pymongo import ASCENDING as ASC

db = client.handykapp


@task
def drop_collection():
    db.formdata.drop()


@task
def spec_collection():
    pass


@task(tags=["RacingResearch"])
def load_horses(horses):
    ret_val = {}
    for horse in horses:
        run_dict = []
        for run in horse.runs:
            run_dict.append(run._asdict())
        horse = horse._asdict()
        horse["runs"] = run_dict

        horse_id = db.formdata.insert_one(horse)
        ret_val[f"{horse['name']} ({horse['country']})"] = horse_id.inserted_id
    return ret_val


@flow
def load_database_afresh():
    drop_collection()
    horses = formdata_transformer()
    load_horses(horses)


if __name__ == "__main__":
    load_database_afresh()  # type: ignore
