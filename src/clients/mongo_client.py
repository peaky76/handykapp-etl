from functools import cache, wraps

from peak_utility.listish import compact
from pymongo import MongoClient

from models import MongoHorse, PreMongoHorse, PyObjectId

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


@cache
def get_horse_id_by_name_and_sex(
    name: str | None, sex: str | None
) -> PyObjectId | None:
    if not name:
        return None

    found_horse = db.horses.find_one({"name": name, "sex": sex}, {"_id": 1})

    if not found_horse:
        raise ValueError(
            f"Could not find {'fe' if sex == 'F' else ''}male horse {name}"
        )

    return found_horse["_id"]


@cache
def get_dam_id(name: str | None) -> PyObjectId | None:
    return get_horse_id_by_name_and_sex(name, "F")


@cache
def get_sire_id(name: str | None) -> PyObjectId | None:
    return get_horse_id_by_name_and_sex(name, "M")
