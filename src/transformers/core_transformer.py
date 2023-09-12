# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

import petl
import re
import yaml
from helpers import get_files, log_validation_problem, stream_file
from horsetalk import (
    Handedness,
    RaceDistance,
    RacecourseContour,
    RacecourseShape,
    RacecourseStyle,
    Surface,
)
from prefect import flow, task
from transformers.validators import validate_in_enum


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["core"]["spaces"]["dir"]


@task(tags=["Core"], name="get_racecourses_csv")
def read_csv():
    csv = next(
        file
        for file in list(get_files(SOURCE))
        if "flatstats_racecourses_edited" in file
    )
    source = petl.MemorySource(stream_file(csv))
    return petl.fromcsv(source)


@task(tags=["Core"])
def transform_racecourses_data(data) -> list:
    used_fields = (
        "Venue",
        "Surface",
        "Shape",
        "Direction",
        "Speed",
        "Contour",
        "RRAbbr",
    )
    return (
        petl.cut(data, used_fields)
        .rename({x: x.lower() for x in used_fields})
        .rename({"venue": "name", "speed": "style", "direction": "handedness"})
        .addfield("references", lambda rec: {"racing_research": rec["rrabbr"]})
        .cutout("rrabbr")
        .dicts()
    )


@task(tags=["Core"])
def validate_racecourses_data(data) -> bool:
    header = (
        "Venue",
        "Surface",
        "Grade",
        "Straight",
        "Shape",
        "Direction",
        "Speed",
        "Contour",
        "Location",
        "RRAbbr",
    )
    constraints = [
        dict(name="venue_str", field="Venue", assertion=lambda x: isinstance(x, str)),
        dict(
            name="surface_valid",
            field="Surface",
            assertion=lambda x: validate_in_enum(x, Surface),
        ),
        dict(name="grade_int", field="Grade", assertion=int),
        dict(
            name="straight_valid",
            field="Straight",
            assertion=lambda x: x is None or re.search(RaceDistance.REGEX, x),
        ),
        dict(
            name="shape_valid",
            field="Shape",
            assertion=lambda x: validate_in_enum(x, RacecourseShape),
        ),
        dict(
            name="direction_valid",
            field="Direction",
            assertion=lambda x: validate_in_enum(x, Handedness),
        ),
        dict(
            name="speed_valid",
            field="Speed",
            assertion=lambda x: validate_in_enum(x, RacecourseStyle),
        ),
        dict(
            name="contour_valid",
            field="Contour",
            assertion=lambda x: validate_in_enum(x, RacecourseContour),
        ),
        dict(
            name="rr_abbr_valid",
            field="RRAbbr",
            assertion=lambda x: isinstance(x, str) and len(x) == 3,
        ),
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@flow
def core_transformer():
    data = read_csv()
    problems = validate_racecourses_data(data)
    for problem in problems.dicts():
        log_validation_problem(problem)
    return transform_racecourses_data(data)


if __name__ == "__main__":
    data = core_transformer()  # type: ignore
    print(data)
