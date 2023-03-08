def parse_horse(horse):
    split_string = horse.replace(")", "").split("(")
    name, country = split_string if len(split_string) == 2 else (horse, None)
    return (name.upper().strip(), country)


def parse_sex(sex):
    return "M" if sex.upper() in ["GELDING", "COLT", "STALLION", "RIG"] else "F"


def parse_weight(weight):
    st, lbs = weight.split("-")
    return int(st) * 14 + int(lbs)
