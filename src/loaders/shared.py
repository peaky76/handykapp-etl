from nameparser import HumanName  # type: ignore


def convert_person(name, source):
    parsed_name = HumanName(name).as_dict()
    parsed_name["display_name"] = {source: name}
    return parsed_name


def select_set(data, key):
    return sorted(list(set([x[key] for x in data])))
