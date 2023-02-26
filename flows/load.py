# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow, task


@task
def drop_database():
    client.drop_database("handykapp")


@task
def load_sires():
    pass  # TODO


@task
def load_dams():
    pass  # TODO


@task
def load_horses():
    pass  # TODO


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
    db = client.handykapp
    collection = db.horses
    id = collection.insert_one(frankel)


@flow
def load_database_afresh():
    drop_database()
    # load_sires()
    # load_dams()
    # load_horses()
    # load_ratings()


if __name__ == "__main__":
    create_sample_database()
