import re
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from helpers import get_files, log_validation_problem, stream_file
from prefect import flow, task
from transformers.parsers import parse_horse
from transformers.parsers import parse_sex as p_sex
import petl
import yaml


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["spaces"]["dir"]


def parse_sex(sex):
    return "M" if sex.upper() in ["GELDING", "COLT", "STALLION", "RIG"] else "F"


def validate_horse(horse):
    if not horse:
        return False

    has_country = bool(re.search("\([A-Z]{2,3}\)", horse))
    return len(horse) <= 30 and has_country


def validate_rating(rating):
    return not rating or (0 <= int(rating) <= 240)


def validate_sex(sex):
    return sex in ["COLT", "FILLY", "GELDING", "STALLION", "MARE", "RIG"]


def validate_year(year):
    return year and 1600 <= int(year) <= 2100


@task(tags=["BHA"], task_run_name="get_{date}_{type}_csv")
def get_csv(type="ratings", date="latest"):
    idx = -1 if date == "latest" else 0
    search_string = "" if date == "latest" else date
    csvs = [csv for csv in get_files(SOURCE) if type in csv and search_string in csv]
    return csvs[idx] if csvs else None


@task(tags=["BHA"])
def read_csv(csv):
    source = petl.MemorySource(stream_file(csv))
    return petl.fromcsv(source)


@task(tags=["BHA"])
def transform_ratings_data(data):
    used_fields = (
        "Name",
        "Year",
        "Sex",
        "Sire",
        "Dam",
        "Trainer",
        "Flat rating",
        "AWT rating",
        "Chase rating",
        "Hurdle rating",
    )
    return (
        petl.cut(data, used_fields)
        .rename({x: x.replace(" rating", "").lower() for x in used_fields})
        .rename({"awt": "aw"})
        .convert({"year": int, "flat": int, "aw": int, "chase": int, "hurdle": int})
        .addfield("country", lambda rec: parse_horse(rec["name"])[1], index=1)
        .convert("name", lambda x: parse_horse(x)[0])
        .addfield("sire_country", lambda rec: parse_horse(rec["sire"])[1])
        .convert("sire", lambda x: parse_horse(x)[0])
        .movefield("sire", -1)
        .addfield("dam_country", lambda rec: parse_horse(rec["dam"])[1])
        .convert("dam", lambda x: parse_horse(x)[0])
        .movefield("dam", -1)
        .addfield("is_gelded", lambda rec: rec["sex"] == "GELDING")
        .convert({"sex": p_sex})
        .dicts()
    )


@task(tags=["BHA"])
def validate_ratings_data(data):
    header = (
        "Name",
        "Year",
        "Sex",
        "Sire",
        "Dam",
        "Trainer",
        "Flat rating",
        "Diff Flat",
        "Flat Clltrl",
        "AWT rating",
        "Diff AWT",
        "AWT Clltrl",
        "Chase rating",
        "Diff Chase",
        "Chase Clltrl",
        "Hurdle rating",
        "Diff Hurdle",
        "Hurdle Clltrl",
    )
    constraints = [
        dict(name="name_str", field="Name", assertion=validate_horse),
        dict(name="year_valid", field="Year", assertion=validate_year),
        dict(name="sex_valid", field="Sex", assertion=validate_sex),
        dict(name="sire_str", field="Sire", assertion=validate_horse),
        dict(name="dam_str", field="Dam", assertion=validate_horse),
        dict(name="trainer_str", field="Trainer", test=str),
        dict(name="flat_valid", field="Flat rating", assertion=validate_rating),
        dict(name="awt_valid", field="AWT rating", assertion=validate_rating),
        dict(name="chase_valid", field="Chase rating", assertion=validate_rating),
        dict(
            name="hurdle_rating_int", field="Hurdle rating", assertion=validate_rating
        ),
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@flow
def bha_transformer():
    csv = get_csv()
    data = read_csv(csv)
    problems = validate_ratings_data(data)
    for problem in problems.dicts():
        log_validation_problem(problem)
    return transform_ratings_data(data)


if __name__ == "__main__":
    data = bha_transformer()
    print(data)
