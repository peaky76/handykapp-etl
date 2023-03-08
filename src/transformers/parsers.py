def parse_horse(horse):
    split_string = horse.replace(")", "").split("(")
    name, country = split_string if len(split_string) == 2 else (horse, None)
    return (name.strip(), country)
