import re

from pendulum import datetime


def parse_code(obstacle, title):
    if obstacle:
        return "NH"

    if "National Hunt" in title or "NH" in title:
        return "NH"

    return "Flat"


def parse_days_since_run(race_date: datetime, days_ago_str: str) -> int | None:
    if not days_ago_str:
        return None

    days_ago_figs = re.sub(r'[a-zA-Z()]', '', days_ago_str)
    days_ago = min([int(x) for x in days_ago_figs.split()])

    return race_date.subtract(days=days_ago)


def parse_horse(horse, default_country=None):
    if not horse:
        return (None, None)

    split_string = horse.replace(")", "").split("(")
    name, country = split_string if len(split_string) == 2 else (horse, default_country)
    return (name.upper().strip(), country)


def parse_obstacle(race_title):
    if not race_title:
        return None

    obstacle_types = {
        r"\bCROSS(-|\s)COUNTRY\b": "CROSS-COUNTRY",
        r"\b(STEEPLE)?CHASE\b": "CHASE",
        r"\bHURDLE\b": "HURDLE",
    }

    for regex, obstacle in obstacle_types.items():
        if re.compile(regex).search(race_title.upper()):
            return obstacle.title()
    return None


def parse_variant(variant):
    conversions = {
        "rnd": "Round",
        "str": "Straight",
    }

    return conversions.get(variant.lower(), variant.title())
