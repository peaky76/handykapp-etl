from pymongo.database import Database


def get_racecourse_id(db: Database, lookup: dict, course: str, surface: str, code: str, obstacle: str) -> (str, dict):

    racecourse_id = lookup.get((course, surface, code, obstacle))

    if not racecourse_id:
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

        if racecourse:
            racecourse_id = racecourse["_id"]
            lookup[(course, surface, code, obstacle)] = racecourse_id

    return (racecourse_id, lookup)