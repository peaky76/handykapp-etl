# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from transformers.formdata_transformer import formdata_transformer
from prefect import flow, task

db = client.handykapp


def create_code_to_course_dict():
    source = db.racecourses.find(
        projection={"_id": 2, "references": {"racing_research": 1}}
    )
    return {
        racecourse["references"]["racing_research"]: racecourse["_id"]
        for racecourse in source
    }


@task
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
def load_formdata_afresh():
    db.formdata.drop()
    horses = formdata_transformer()
    load_horses(horses)


if __name__ == "__main__":
    load_formdata_afresh()  # type: ignore
