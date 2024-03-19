# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow
from processors.horse_processor import HorseProcessor
from transformers.bha_transformer import bha_transformer

from .loader import Loader

db = client.handykapp

@flow
def load_bha():
    loader = Loader(bha_transformer(), HorseProcessor())
    loader.load()

@flow
def load_bha_afresh():
    db.horses.drop()
    db.people.drop()
    load_bha()

if __name__ == "__main__":
    load_bha_afresh()
