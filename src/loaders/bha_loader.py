# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow
from processors.horse_processor import HorseProcessor
from transformers.bha_transformer import bha_transformer

db = client.handykapp

@flow
def load_bha():
    data = bha_transformer()
    h = HorseProcessor()()
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
def load_bha_afresh():
    db.horses.drop()
    db.people.drop()
    load_bha()

if __name__ == "__main__":
    load_bha_afresh()
