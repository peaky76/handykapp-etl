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
def load_formdata(formdata):
    ret_val = {}
    for entry in formdata:
        run_dict = []
        for run in entry.runs:
            run_dict.append(run._asdict())
        entry = entry._asdict()
        entry["runs"] = run_dict

        entry_id = db.formdata.insert_one(entry)
        ret_val[f"{entry['name']} ({entry['country']})"] = entry_id.inserted_id
    return ret_val


@flow
def load_formdata_afresh():
    db.formdata.drop()
    formdata = formdata_transformer()
    load_formdata(formdata)


if __name__ == "__main__":
    load_formdata_afresh()  # type: ignore
