import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from models.mongo_horse import MongoHorse
from horsetalk import Gender  # type: ignore
from helpers import get_files, log_validation_problem, stream_file
from prefect import flow, task
from transformers.validators import (
    validate_horse,
    validate_rating,
    validate_sex,
    validate_year,
)
import petl  # type: ignore
import yaml


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["spaces"]["dir"]


@task(tags=["BHA"], task_run_name="get_{date}_{csv_type}_csv")
def get_csv(csv_type="ratings", date="latest"):
    idx = -1 if date == "latest" else 0
    search_string = "" if date == "latest" else date
    csvs = [
        csv
        for csv in list(get_files(SOURCE))
        if csv_type in csv and search_string in csv
    ]
    return csvs[idx] if csvs else None


@task(tags=["BHA"])
def read_csv(csv):
    source = petl.MemorySource(stream_file(csv))
    return petl.fromcsv(source)


@task(tags=["BHA"])
def transform_ratings_data(data) -> list[MongoHorse]:
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
    rating_types = ["flat", "aw", "chase", "hurdle"]
    return (
        petl.cut(data, used_fields)
        .rename({x: x.replace(" rating", "").lower() for x in used_fields})
        .rename({"awt": "aw"})
        .convert({"year": int, "flat": int, "aw": int, "chase": int, "hurdle": int})
        .addfield(
            "operations",
            lambda rec: [{"type": "gelding", "date": None}]
            if rec["sex"] == "GELDING"
            else None,
        )
        .convert({"sex": lambda x: Gender[x].sex.name[0]})
        .addfield("ratings", lambda rec: {rtg: rec[rtg] for rtg in rating_types})
        .cutout(*rating_types)
        .dicts()
    )


@task(tags=["BHA"])
def validate_ratings_data(data) -> bool:
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
    data = bha_transformer()  # type: ignore
    print(data)
