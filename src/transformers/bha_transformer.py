import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from helpers import get_files, read_file, stream_file
from prefect import flow, task
import petl
import yaml


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["spaces"]["dir"]


def parse_horse(horse):
    split_string = horse.replace(")", "").split(" (")
    name, country = split_string if len(split_string) == 2 else (horse, None)
    return (name, country)


def parse_sex(sex):
    return "M" if sex.upper() in ["GELDING", "COLT", "STALLION"] else "F"


def validate_rating(rating):
    return not rating or (0 < int(rating) < 240)


def validate_sex(sex):
    return sex in ["COLT", "FILLY", "GELDING", "STALLION", "MARE"]


def validate_year(year):
    return 1600 <= int(year) <= 2100


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
def transform_ratings_csv(csv):
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
        petl.fromcsv(csv)
        .cut(used_fields)
        .rename({x: x.replace(" rating", "").lower() for x in used_fields})
        .rename({"awt": "aw"})
        .convert({"year": int, "flat": int, "aw": int, "chase": int, "hurdle": int})
    )


@task(tags=["BHA"])
def validate_ratings_input(data):
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
        dict(name="name_str", field="Name", test=str),
        dict(name="year_valid", field="Year", test=validate_year),
        dict(name="sex_valid", field="Sex", test=validate_sex),
        dict(name="sire_str", field="Sire", test=str),
        dict(name="dam_str", field="Dam", test=str),
        dict(name="trainer_str", field="Trainer", test=str),
        dict(name="flat_valid", field="Flat rating", test=validate_rating),
        dict(name="awt_valid", field="AWT rating", test=validate_rating),
        dict(name="chase_valid", field="Chase rating", test=validate_rating),
        dict(name="hurdle_rating_int", field="Hurdle rating", test=validate_rating),
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@task(tags=["BHA"])
def validate_ratings_data(data):
    header = (
        "name",
        "year",
        "sex",
        "sire",
        "dam",
        "trainer",
        "flat",
        "aw",
        "chase",
        "hurdle",
    )
    constraints = [
        dict(name="name_str", field="name", test=str),
        dict(name="year_int", field="year", test=int),
        dict(
            name="year_sensible",
            field="year",
            assertion=lambda x: 1600 <= int(x) <= 2100,
        ),
        dict(
            name="sex_valid",
            field="sex",
            test=lambda x: x in ["COLT", "FILLY", "GELDING", "STALLION", "MARE"],
        ),
        dict(name="sire_str", field="sire", test=str),
        dict(name="dam_str", field="dam", test=str),
        dict(name="trainer_str", field="trainer", test=str),
        dict(
            name="flat_int",
            field="flat",
            assertion=lambda x: x is None or isinstance(x, int),
        ),
        dict(
            name="aw_int",
            field="aw",
            assertion=lambda x: x is None or isinstance(x, int),
        ),
        dict(
            name="chase_int",
            field="chase",
            assertion=lambda x: x is None or isinstance(x, int),
        ),
        dict(
            name="hurdle_int",
            field="hurdle",
            assertion=lambda x: x is None or isinstance(x, int),
        ),
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@task(tags=["BHA"])
def select_dams(data):
    return sorted(list(set([x["dam"] for x in data])))


@task(tags=["BHA"])
def select_sires(data):
    return sorted(list(set([x["sire"] for x in data])))


@task(tags=["BHA"])
def select_trainers(data):
    return sorted(list(set([x["trainer"] for x in data])))


@flow
def bha_transformer():
    source = petl.MemorySource(stream_file(get_csv()))
    data = transform_ratings_csv(source)
    problems = validate_ratings_data(data)
    print(problems.lookall())
    sires = select_sires(data.dicts())
    dams = select_dams(data.dicts())
    # print(sires)


if __name__ == "__main__":
    bha_transformer()
