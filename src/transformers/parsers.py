import re


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
            return obstacle
    return None
