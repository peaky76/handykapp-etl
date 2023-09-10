import pendulum
import re
from horsetalk import AWGoingDescription, Gender, TurfGoingDescription


def validate_class(race_class):
    return race_class == "" or 0 < int(race_class) < 8


def validate_date(date):
    try:
        pendulum.parse(date)
        return True
    except pendulum.exceptions.ParserError:
        return False
    except TypeError:
        return False


def validate_distance(distance):
    pattern = r"^(([1-4]m)(\s)?([1-7]f)?|([4-7]f))$"
    return bool(re.match(pattern, distance)) if distance else False


def validate_going(going):
    if not going:
        return False

    goings = [g.name for d in [TurfGoingDescription, AWGoingDescription] for g in d]
    going = going.upper().replace(")", "").replace(" IN PLACES", "").split(" (")
    return any(g in going[0] for g in goings) and (
        any(g in going[1] for g in goings) if len(going) == 2 else True
    )


def validate_horse(horse):
    if not horse:
        return False

    has_country = bool(re.search("\\([A-Z]{2,3}\\)", horse))
    return len(horse) <= 30 and has_country


def validate_in_enum(value, enum):
    return value and value.lower() in [e.name.lower() for e in enum]


def validate_prize(prize):
    pattern = r"^[£\$€¥]\d{1,3}(,)*\d{1,3}$"
    return bool(re.match(pattern, prize)) if prize else False


def validate_rating(rating):
    return not rating or (0 <= int(rating) <= 240)


def validate_sex(sex):
    return validate_in_enum(sex, Gender)


def validate_weight(weight):
    pattern = r"^\d{1,2}-(0\d|1[0-3])$"
    return bool(re.match(pattern, weight)) if weight else False


def validate_year(year):
    return year and 1600 <= int(year) <= 2100
