# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow, get_run_logger
from processors.racecourse_processor import racecourse_processor
from transformers.core_transformer import core_transformer

db = client.handykapp

@flow
def load_racecourses():
    logger = get_run_logger()
    logger.info("Starting racecourse loader")

    racecourses = core_transformer()

    r = racecourse_processor()
    next(r)

    for racecourse in racecourses:
        r.send(racecourse)
    
    r.close()

@flow
def load_racecourses_afresh():
    db.racecourses.drop()
    load_racecourses()


if __name__ == "__main__":
    load_racecourses_afresh()  # type: ignore
