# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import re
from typing import Generator, List, Optional

import petl  # type: ignore
import tomllib
from helpers import get_files, stream_file
from horsetalk import (
    Handedness,
    JumpCategory,
    RacecourseContour,
    RacecourseShape,
    RacecourseStyle,
    RaceDistance,
    Surface,
)
from models import Racecourse
from peak_utility.text.case import snake  # type: ignore
from prefect import task

from transformers.transformer import Transformer
from transformers.validators import validate_in_enum

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["core"]["spaces_dir"]


@task(tags=["Core"], name="get_racecourses_csvs")
def read_csvs() -> Generator[Optional[petl.Table], None, None]:
    for csv in list(get_files(SOURCE)):
        if "edited" in csv:
            source = petl.MemorySource(stream_file(csv))
            yield petl.fromcsv(source)


@task(tags=["Core"])
def transform_racecourses_data(data: petl.Table) -> List[Racecourse]:
    used_fields = (
        "Name",
        "Formal Name",
        "Surface",
        "Obstacle",
        "Shape",
        "Direction",
        "Speed",
        "Contour",
        "Country",
        "RR Abbr",
    )
    racecourse_dicts = (
        petl.cut(data, used_fields)
        .rename({x: snake(x.lower()) for x in used_fields})
        .rename({"speed": "style", "direction": "handedness"})
        .addfield("code", lambda rec: "NH" if rec["obstacle"] else "Flat", index=2)
        .addfield("references", lambda rec: {"racing_research": rec["rr_abbr"]})
        .addfield("source", "core")
        .convert("formal_name", lambda x, rec: x if x else rec["name"], pass_row=True)
        .convert("obstacle", lambda x: x.replace("Steeple", "") if x else x)
        .convert(
            ("obstacle", "surface", "shape", "style", "handedness", "contour"), lambda x: x.title() if x else None
        )
        .replace("handedness", "Straight", "Neither")
        .cutout("rr_abbr")
        .dicts()
    )
    return [Racecourse(**racecourse) for racecourse in racecourse_dicts]


@task(tags=["Core"])
def validate_racecourses_data(data: petl.Table) -> petl.transform.validation.ProblemsView:
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
        "Country",
        "RR Abbr",
    )
    constraints = [
        {
            "name": "name_str",
            "field": "Name",
            "assertion": lambda x: isinstance(x, str),
        },
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


class CoreRacecoursesTransformer(Transformer[Racecourse]):
    def __init__(self, source_data: petl.Table):
        super().__init__(
            source_data=source_data,
            validator=validate_racecourses_data,
            transformer=transform_racecourses_data,
        )


if __name__ == "__main__":
    data = CoreRacecoursesTransformer().transform()  # type: ignore
    print(data)