# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow, task


@task
def drop_database():
    client.drop_database("handykapp")


@flow
def create_fresh_database():
    frankel = {
        "name": "Frankel",
        "country": "GB",
        "yob": 2008,
    }

    drop_database()
    db = client.handykapp
    collection = db.horses
    id = collection.insert_one(frankel)


if __name__ == "__main__":
    create_fresh_database()
