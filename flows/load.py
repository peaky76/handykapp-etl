# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow, task


@flow
def create_fresh_database():
    client.drop_database("handykapp")


if __name__ == "__main__":
    create_fresh_database()
