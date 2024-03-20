# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import re
from typing import List

import petl  # type: ignore
import tomllib
from helpers import log_validation_problem, stream_file
from models import RatedJockey
from nameparser import HumanName  # type: ignore
from peak_utility.listish import compact  # type: ignore
from peak_utility.text.case import normal, snake  # type: ignore
from prefect import flow, task

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["core"]["spaces_dir"]
YEARS = [str(x) for x in range(2000, 2024)]

def read_csv(csv):
    source = petl.MemorySource(stream_file(csv))
    return petl.fromcsv(source)

@task(tags=["Core"])
def transform_jockey_ratings_data(data: petl.Table) -> List[RatedJockey]:
    used_fields = ("Name", *YEARS)
    rtgs_dicts = ( 
        petl.cut(data, used_fields)
        .rename({x: snake(x.lower()) for x in used_fields})
        .convert("name", lambda x: str(HumanName(normal(" ".join([*x.split(" ")[1:], x.split(" ")[0]])).title())))
        .addfield("role", "jockey")
        .addfield("ratings", lambda rec: compact({ str(y): rec[y] or None for y in YEARS }))
        .addfield("references", lambda rec: {"racing_research": rec["name"]})
        .cut("name", "role", "ratings", "references")
        .dicts()
    )
    return [RatedJockey(**rtgs) for rtgs in rtgs_dicts]


@task(tags=["Core"])
def validate_jockey_ratings_data(data: petl.Table) -> petl.transform.validation.ProblemsView:
    header = ("Name",*YEARS)
    constraints = [
        {"name": "name_str", "field": "Name", "test": str},
        *[{"name": f'{y}_int', "field": y, "assertion": lambda x: len(x) == 0 or re.compile(r"^[0-2]?\d{1}\.\d{1}\*{0,3}$").match(x)} for y in YEARS],        
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@flow
def jockey_ratings_transformer() -> List[RatedJockey]:
    data = read_csv(f"{SOURCE}jockeys/jockey_ratings_historic.csv")

    if (problems := validate_jockey_ratings_data(data)):
        for problem in problems.dicts():
            log_validation_problem(problem)
    return transform_jockey_ratings_data(data)


if __name__ == "__main__":
    data = jockey_ratings_transformer()
    print(data)