import re
import pendulum


def parse_going(going):
    if not going:
        return {"main": None, "secondary": None}

    going = going.upper().replace(")", "").replace(" IN PLACES", "").split(" (")
    return {"main": going[0], "secondary": going[1] if len(going) == 2 else None}


def parse_horse(horse, default_country=None):
    if not horse:
        return (None, None)

    split_string = horse.replace(")", "").split("(")
    name, country = split_string if len(split_string) == 2 else (horse, default_country)
    return (name.upper().strip(), country)


def parse_obstacle(race_title):
    obstacle_types = {
        r"\bCROSS(-|\s)COUNTRY\b": "CROSS-COUNTRY",
        r"\b(STEEPLE)?CHASE\b": "CHASE",
        r"\bHURDLE\b": "HURDLE",
    }

    for regex, obstacle in obstacle_types.items():
        if re.compile(regex).search(race_title.upper()):
            return obstacle
    return None


def parse_sex(sex):
    return "M" if sex.upper() in ["GELDING", "COLT", "STALLION", "RIG"] else "F"


def parse_weight(weight):
    st, lbs = weight.split("-")
    return int(st) * 14 + int(lbs)


def parse_yards(distance_description):
    if not distance_description:
        return 0

    if "m" in distance_description:
        parts = distance_description.split("m")
        miles = int(parts[0])
        furlongs = int(parts[1].split("f")[0]) if "f" in distance_description else 0
    else:
        parts = distance_description.split("f")
        miles = 0
        furlongs = int(parts[0]) if "f" in distance_description else 0

    return (miles * 8 + furlongs) * 220


def yob_from_age(age, date=pendulum.now()):
    return date.year - age
