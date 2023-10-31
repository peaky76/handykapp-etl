# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow, task
from pymongo import ASCENDING as ASC

from loaders.bha_loader import load_bha
from loaders.racecourse_loader import load_racecourses
from loaders.theracingapi_loader import load_theracingapi_data

db = client.handykapp


@task
def drop_database():
    client.drop_database("handykapp")


@task
def spec_database():
    db.horses.create_index(
        [("name", ASC), ("country", ASC), ("year", ASC)], unique=True
    )
    db.horses.create_index("name")
    db.people.create_index(
        [("last", ASC), ("first", ASC), ("middle", ASC)], unique=True
    )
    db.racecourses.create_index(
        [("name", ASC), ("country", ASC), ("obstacle", ASC), ("surface", ASC)],
        unique=True,
    )
    db.racecourses.create_index("name")
    db.races.create_index([("racecourse", ASC), ("datetime", ASC)], unique=True)
    db.races.create_index("result.horse")


@flow
def load_database_afresh():
    drop_database()
    spec_database()
    load_racecourses()
    load_bha()
    load_theracingapi_data()
    # load_formdata()
    # load_formdata_horses()


if __name__ == "__main__":
    load_database_afresh()  # type: ignore
