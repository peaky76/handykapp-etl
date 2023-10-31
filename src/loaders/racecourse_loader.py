# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow
from transformers.core_transformer import core_transformer

from loaders.adders import add_racecourse

db = client.handykapp


@flow
def load_racecourses():
    db.racecourses.drop()
    racecourses = core_transformer()
    for racecourse in racecourses:
        add_racecourse(racecourse)


if __name__ == "__main__":
    load_racecourses()  # type: ignore
