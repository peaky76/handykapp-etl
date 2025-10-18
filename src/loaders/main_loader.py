# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
from prefect import flow, task
from pymongo import ASCENDING as ASC

from clients import mongo_client as client
from loaders.bha_loader import load_bha_data
from loaders.formdata_loader import load_formdata
from loaders.racecourse_loader import load_racecourses
from loaders.rapid_horseracing_loader import load_rapid_horseracing_entries
from loaders.theracingapi_loader import load_theracingapi_data

db = client.handykapp


@task
def drop_database():
    client.drop_database("handykapp")


@task
def spec_database():
    db.formdata.create_index(
        [("name", ASC), ("country", ASC), ("year", ASC)], unique=True
    )
    db.horses.create_index(
        [("name", ASC), ("country", ASC), ("year", ASC)],
        unique=True,
        partialFilterExpression={
            "$and": [{"country": {"$type": ["string"]}}, {"year": {"$type": ["int"]}}]
        },
    )
    db.horses.create_index(
        [("name", ASC), ("country", ASC), ("year", ASC), ("sex", ASC)], unique=True
    )
    db.horses.create_index("name")
    db.horses.create_index("sire")
    db.horses.create_index("dam")
    db.people.create_index(
        [("last", ASC), ("first", ASC), ("middle", ASC)], unique=True
    )
    db.people.create_index(
        "references.racing_research",
        unique=True,
        partialFilterExpression={"references.racing_research": {"$exists": True}},
    )
    db.racecourses.create_index(
        [("name", ASC), ("country", ASC), ("obstacle", ASC), ("surface", ASC)],
        unique=True,
    )
    db.racecourses.create_index("name")
    db.races.create_index([("racecourse", ASC), ("datetime", ASC)], unique=True)
    db.races.create_index("runners.horse")


@flow
def nuclear_reload():
    drop_database()
    spec_database()
    load_racecourses()
    switch_date = pendulum.parse("2023-03-11").date()
    load_rapid_horseracing_entries(until_date=switch_date)
    load_theracingapi_data()
    load_bha_data()
    load_formdata()


if __name__ == "__main__":
    nuclear_reload()  # type: ignore
