# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import re

import petl  # type: ignore
import tomllib
from helpers import get_files, log_validation_problem, stream_file
from horsetalk import (  # type: ignore
    Handedness,
    JumpCategory,
    RacecourseContour,
    RacecourseShape,
    RacecourseStyle,
    RaceDistance,
    Surface,
)
from peak_utility.text.case import snake  # type: ignore
from prefect import flow, task

from transformers.validators import validate_in_enum

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["core"]["spaces_dir"]


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
        "Name",
        "Formal Name",
        "Surface",
        "Obstacle",
        "Shape",
        "Direction",
        "Speed",
        "Contour",
        "RR Abbr",
    )
    return (
        petl.cut(data, used_fields)
        .rename({x: snake(x.lower()) for x in used_fields})
        .rename({"speed": "style", "direction": "handedness"})
        .addfield("country", "GB", index=2)
        .addfield("code", "Flat", index=3)
        .addfield("references", lambda rec: {"racing_research": rec["rr_abbr"]})
        .convert(
            ("surface", "shape", "style", "handedness", "contour"), lambda x: x.title()
        )
        .convert("obstacle", lambda x: x or None)
        .cutout("rr_abbr")
        .dicts()
    )


@task(tags=["Core"])
def validate_racecourses_data(data) -> bool:
    header = (
        "Name",
        "Formal Name",
        "Surface",
        "Obstacle",
        "Grade",
        "Straight",
        "Shape",
        "Direction",
        "Speed",
        "Contour",
        "Location",
        "RR Abbr",
    )
    constraints = [
        {"name": "name_str", "field": "Name", "assertion": lambda x: isinstance(x, str)},
        {
            "name": "formal_name_str",
            "field": "Formal Name",
            "assertion": lambda x: isinstance(x, str),
        },
        {
            "name": "surface_valid",
            "field": "Surface",
            "assertion": lambda x: validate_in_enum(x, Surface),
        },
        {
            "name": "obstacle_valid",
            "field": "Obstacle",
            "assertion": lambda x: validate_in_enum(x, JumpCategory) or not x,
        },
        {"name": "grade_int", "field": "Grade", "assertion": int},
        {
            "name": "straight_valid",
            "field": "Straight",
            "assertion": lambda x: not x or re.search(RaceDistance.REGEX, x),
        },
        {
            "name": "shape_valid",
            "field": "Shape",
            "assertion": lambda x: validate_in_enum(x, RacecourseShape),
        },
        {
            "name": "direction_valid",
            "field": "Direction",
            "assertion": lambda x: validate_in_enum(x, Handedness),
        },
        {
            "name": "speed_valid",
            "field": "Speed",
            "assertion": lambda x: validate_in_enum(x, RacecourseStyle),
        },
        {
            "name": "contour_valid",
            "field": "Contour",
            "assertion": lambda x: validate_in_enum(x, RacecourseContour),
        },
        {
            "name": "rr_abbr_valid",
            "field": "RR Abbr",
            "assertion": lambda x: isinstance(x, str) and len(x) == 3,
        },
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
