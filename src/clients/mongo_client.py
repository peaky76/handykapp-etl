from functools import wraps

from peak_utility.listish import compact
from pymongo import MongoClient

from models import MongoHorse, PreMongoHorse

mongo_client = MongoClient("mongodb://localhost:27017/")  # type: ignore


db = mongo_client.handykapp


def cache_if_found(func):
    cache = {}

    @wraps(func)
    def wrapper(*args, **kwargs):
        key = (args, tuple(sorted(kwargs.items())))
        if key in cache:
            return cache[key]
        result = func(*args, **kwargs)
        if result is not None:  # Only cache when we find something
            cache[key] = result
        return result

    return wrapper


@cache_if_found
def get_horse(horse: PreMongoHorse) -> MongoHorse | None:
    horse = db.horses.find_one(
        compact(
            {
                "name": horse.name,
                "country": horse.country,
                "year": horse.year,
                "sex": horse.sex,
            }
        ),
    )
    if horse:
        return horse

    return db.horses.find_one(
        compact(
            {
                "name": horse.name,
                "country": horse.country,
                "year": horse.year,
                "sex": None,
            }
        ),
    )
