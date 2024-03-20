# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client
from prefect import flow
from processors.database_processors import HorseProcessor
from transformers.bha_transformer import BHATransformer

from .loader import Loader

db = client.handykapp

@flow
def load_bha():
    data = BHATransformer().transform()
    loader = Loader(data, HorseProcessor())
    loader.load()

@flow
def load_bha_afresh():
    db.horses.drop()
    db.people.drop()
    load_bha()

if __name__ == "__main__":
    load_bha_afresh()
