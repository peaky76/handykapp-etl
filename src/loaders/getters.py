from functools import cache

from clients import mongo_client as client

db = client.handykapp


@cache
def lookup_racecourse_id(course: str, surface: str, code: str, obstacle: str) -> str:
    surface_options = ["Tapeta", "Polytrack"] if surface == "AW" else ["Turf"]
    racecourse = db.racecourses.find_one(
        {
            "name": course.title(),
            "surface": {"$in": surface_options},
            "code": code,
            "obstacle": obstacle,
        },
        {"_id": 1},
    )
    return racecourse["_id"] if racecourse else None
