# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow
from processors.racecourse_processor import RacecourseProcessor
from transformers.core_transformer import core_transformer

db = client.handykapp

@flow
def load_racecourses():
    data = core_transformer()
    p = RacecourseProcessor()
    r = p.start()
    next(r)

    for racecourse in data:
        r.send(racecourse)
    
    r.close()

@flow
def load_racecourses_afresh():
    db.racecourses.drop()
    load_racecourses()


if __name__ == "__main__":
    load_racecourses_afresh()  # type: ignore
