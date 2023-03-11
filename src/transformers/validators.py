import re
import pendulum


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

    goings = [
        "HARD",
        "FIRM",
        "GOOD TO FIRM",
        "GOOD",
        "GOOD TO SOFT",
        "SOFT",
        "SOFT TO HEAVY",
        "HEAVY",
        "STANDARD",
        "STANDARD TO SLOW",
        "SLOW",
        "YIELDING",
    ]
    going = going.upper().replace(")", "").replace(" IN PLACES", "").split(" (")
    return going[0] in goings and (going[1] in goings if len(going) == 2 else True)


def validate_handicap(title):
    return any(word in title.upper() for word in ["HANDICAP", "H'CAP"])


def validate_prize(prize):
    pattern = r"^[£|\$]\d{1,3}(,)*\d{1,3}$"
    return bool(re.match(pattern, prize)) if prize else False


def validate_weight(weight):
    pattern = r"^\d{1,2}-(0\d|1[0-3])$"
    return bool(re.match(pattern, weight)) if weight else False
