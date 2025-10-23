from functools import lru_cache

from peak_utility.listish import compact
from pymongo import MongoClient

from models import MongoHorse, PreMongoHorse

mongo_client = MongoClient("mongodb://localhost:27017/")  # type: ignore


db = mongo_client.handykapp


@lru_cache(maxsize=15000)
def _get_horse_dict(horse: PreMongoHorse) -> dict | None:
    return db.horses.find_one(
        compact(
            {
                "name": horse.name,
                "country": horse.country,
                "year": horse.year,
                "sex": horse.sex,
            }
        ),
    )


def get_horse(horse: PreMongoHorse) -> MongoHorse | None:
    db_horse = _get_horse_dict(horse)
    if not db_horse:
        return None
    return MongoHorse.model_validate(db_horse)
