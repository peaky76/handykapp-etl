# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from clients import mongo_client as client

db = client.handykapp


def add_horse(horse):
    horse_id = db.horses.insert_one(horse)
    return horse_id.inserted_id


def add_person(person):
    person_id = db.people.insert_one(person)
    return person_id.inserted_id

