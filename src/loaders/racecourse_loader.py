# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow
from clients import mongo_client as client
from loaders.adders import add_racecourse
from transformers.core_transformer import core_transformer

db = client.handykapp


@flow
def load_racecourses():
    db.racecourses.drop()
    racecourses = core_transformer()
    for racecourse in racecourses:
        add_racecourse(racecourse)


if __name__ == "__main__":
    load_racecourses()  # type: ignore
