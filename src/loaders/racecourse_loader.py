# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow
from transformers.core_transformer import core_transformer

db = client.handykapp


@flow
def load_racecourses():
    db.racecourses.drop()
    racecourses = core_transformer()
    db.racecourses.insert_many(racecourses)


if __name__ == "__main__":
    load_racecourses()  # type: ignore
