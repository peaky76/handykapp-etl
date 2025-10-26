from functools import cache, wraps

import pendulum
from horsetalk import Surface
from peak_utility.listish import compact
from pymongo import MongoClient

from helpers import apply_newmarket_workaround
from models import PreMongoHorse, PreMongoRace

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


@cache
def rr_code_to_course_dict():
    source = db.racecourses.find(
        projection={"_id": 1, "surface": 1, "references.racing_research": 1}
    )
    return {
        (
            racecourse["references"]["racing_research"],
            Surface[racecourse["surface"]],
        ): racecourse["_id"]
        for racecourse in source
    }


@cache
def get_all_racecourses():
    return list(
        db.racecourses.find(
            {},
            {
                "name": 1,
                "formal_name": 1,
                "surface": 1,
                "code": 1,
                "obstacle": 1,
                "references": 1,
            },
        )
    )


def get_racecourse_id(race: PreMongoRace, source: str) -> str | None:
    if source == "racing_research":
        return rr_code_to_course_dict().get((race.course, race.surface))

    racecourses = get_all_racecourses()
    surface_options = (
        ["Tapeta", "Polytrack"]
        if race.surface == "AW" or race.surface == "All Weather"
        else [race.surface]
        if race.surface
        else ["Tapeta", "Polytrack", "Sand", "Turf"]
    )
    course_name = race.course.lower().replace("(", "").replace(")", "").strip()
    if course_name == "newmarket":
        course_name = apply_newmarket_workaround(
            pendulum.parse(str(race.datetime))  # type: ignore[arg-type]
        ).lower()

    for racecourse in racecourses:
        if (
            (
                racecourse["name"].lower() == course_name
                or racecourse["formal_name"].lower() == course_name
            )
            and racecourse.get("surface") in surface_options
            and racecourse.get("code") == race.code
            and (
                racecourse.get("obstacle") == race.obstacle
                or (racecourse.get("obstacle") is None and race.obstacle is None)
            )
        ):
            return racecourse["_id"]

    return None
