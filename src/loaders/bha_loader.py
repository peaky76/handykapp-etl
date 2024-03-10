# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from typing import List, Optional

from clients import mongo_client as client
from models.process_horse import ProcessHorse
from prefect import flow, get_run_logger
from processors.horse_processor import horse_processor
from transformers.bha_transformer import bha_transformer

db = client.handykapp

@flow
def load_bha(data: Optional[List[ProcessHorse]] = None):
    logger = get_run_logger()
    logger.info("Starting bha loader")

    if data is None:
        data = bha_transformer()

    h = horse_processor()
    next(h)

    sires = list({horse.sire for horse in data if horse.sire})
    dams = list({horse.dam for horse in data if horse.dam})

    for sire in sires:
        h.send(sire)

    for dam in dams:
        h.send(dam)
    
    for horse in data:
        h.send(horse)

    h.close()

@flow
def load_bha_afresh(data: Optional[List[ProcessHorse]] = None):
    if data is None:
        data = bha_transformer()

    db.horses.drop()
    db.people.drop()
    load_bha(data)


if __name__ == "__main__":
    load_bha_afresh()
