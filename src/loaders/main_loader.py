# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow, task
from pymongo import ASCENDING as ASC
from loaders.bha_loader import load_bha_afresh
from loaders.racecourse_loader import load_racecourses
from loaders.formdata_loader import load_formdata_horses

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
    db.races.create_index(
        [("racecourse", ASC), ("date", ASC), ("time", ASC)], unique=True
    )
    db.races.create_index("result.horse")


@flow
def load_database_afresh():
    drop_database()
    spec_database()
    load_bha_afresh()
    load_racecourses()
    load_formdata_horses()


if __name__ == "__main__":
    load_database_afresh()  # type: ignore
