def parse_horse(horse):
    split_string = horse.replace(")", "").split("(")
    name, country = split_string if len(split_string) == 2 else (horse, None)
    return (name.strip(), country)


def parse_sex(sex):
    return "M" if sex.upper() in ["GELDING", "COLT", "STALLION", "RIG"] else "F"
