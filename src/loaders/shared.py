from nameparser import HumanName
from transformers.parsers import parse_horse  # type: ignore


def convert_person(name, source):
    parsed_name = HumanName(name).as_dict()
    parsed_name["references"] = {source: name}
    return parsed_name


def convert_value_to_id(horse, value, lookup):
    if value:
        horse[value] = (
            lookup.get(parse_horse(horse[value]), None)
            if value in ["sire", "dam"]
            else lookup.get(horse[value], None)
        )
    return horse


def select_set(data, key):
    return sorted(list(set([x[key] for x in data])))
