from functools import wraps

from peak_utility.listish import compact
from pymongo import MongoClient

from models import PreMongoHorse

mongo_client = MongoClient("mongodb://localhost:27017/")  # type: ignore


db = mongo_client.handykapp


def cache_if_found(maxsize=None):
    def decorator(func):
        cache = {}

        @wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            if key in cache:
                return cache[key]
            result = func(*args, **kwargs)
            if result is not None:  # Only cache when we find something
                cache[key] = result
                # Limit cache size
                if maxsize and len(cache) > maxsize:
                    # Remove oldest 1000 entries
                    for _ in range(1000):
                        cache.pop(next(iter(cache)))
            return result

        return wrapper

    return decorator


@cache_if_found(maxsize=50000)
def get_horse(horse: PreMongoHorse) -> dict | None:
    search = db.horses.find_one
    base = {"name": horse.name, "country": horse.country, "year": horse.year}

    result = search(base)
    if result:
        return result

    result = search(compact(base) | {"sex": horse.sex})
    if result:
        return result

    name_regex = "".join(char + "'?" if char.isalpha() else char for char in horse.name)
    return search(base | {"name": {"$regex": f"^{name_regex}$", "$options": "i"}})
