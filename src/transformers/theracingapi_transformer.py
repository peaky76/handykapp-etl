# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


from typing import List, Literal

import pendulum
import petl  # type: ignore
import tomllib
from horsetalk import (  # type: ignore
    AgeRestriction,
    CoatColour,
    Gender,
    Headgear,
    HorseAge,
    RaceClass,
    RaceDistance,
    RaceGrade,
)
from models import Declaration, Entry

from transformers.parsers import parse_code, parse_obstacle
from transformers.transformer import Transformer
from transformers.validators import (
    validate_date,
    validate_pattern,
    validate_time,
    validate_weight,
)

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]


def build_datetime(date_str: str, time_str: str) -> str:
    hour, minute = time_str.split(":")
    hour = str(h + 12 if (h := int(hour)) < 11 else h)
    return pendulum.parse(f"{date_str} {hour}:{minute}").isoformat()  # type: ignore

def generate_rating_object(figure: int, obstacle: str | None, surface='Turf'):
    key = obstacle.lower() if obstacle else 'aw' if surface.lower() != "turf" else "flat"
    return { key: figure }

def generate_min_max(restriction_value: str, restriction_type: Literal["age", "rating"]) -> dict:
    if restriction_type == 'rating':
        (the_min, the_max) = restriction_value.split("-")
    else: 
        restriction = AgeRestriction(restriction_value)
        the_min = restriction.minimum
        the_max = restriction.maximum

    return {
        "min": the_min,
        "max": the_max,
    }


# @task(tags=["RapidAPI"])
def transform_horse_data(data: petl.Table, race_date: pendulum.datetime = pendulum.now(), obstacle: str | None = None, surface: str ="Turf") -> Entry:
    horse_dict = (
        petl.rename(
            data,
            {
                "horse": "name",
                "region": "country",
                "number": "saddlecloth",
                "lbs": "lbs_carried",
                "ofr": "official_rating",
                "form": "prev_form"
            },
        )
        .convert({
            "name": lambda x: x.upper(),
            "sex": lambda x: Gender[x].sex.name[0],
            "age": int,
            "colour": lambda x: CoatColour[x].name.title(),
            # Not enough info available for Horse Core objects
            # "sire": lambda x: {"name": x.upper()},
            # "dam": lambda x: {"name": x.upper()},
            # "damsire": lambda x: {"name": x.upper()},
            "saddlecloth": int,
            "draw": int,
            "lbs_carried": int,
            "headgear": lambda x: Headgear[x].name.title() if x else None,
        })
        .addfield(
            "year",
            lambda rec: HorseAge(rec["age"], context_date=race_date)._official_dob.year,
            index=3,
        )
        .addfield(
            "allowance",
            lambda rec: int(rec["jockey"].split("(")[1].split(")")[0])
            if "(" in rec["jockey"]
            else 0,
        )
        .addfield("source", "theracingapi")
        .convert("jockey", lambda x: {"name": x.split("(")[0].strip(), "role": "jockey", "references": {"theracingapi": x.split("(")[0].strip()}})
        .convert("trainer", lambda x: {"name": x, "role": "trainer", "references": {"theracingapi": x}})
        .convert("official_rating", lambda x: int(x) if x and x != "-" else None)
        .cutout("sex_code", "last_run", "age", "sire", "dam", "damsire")
        .dicts()[0]
    )
    return Entry(**horse_dict)

# @task(tags=["RapidAPI"])
def transform_races_data(data: petl.Table) -> List[Declaration]:
    race_dicts = (
        petl.rename(
            data,
            {
                "race_name": "title",
                "age_band": "age_restriction",
                "rating_band": "rating_restriction",
                "going": "going_description",
                "pattern": "race_grade",
                "distance_f": "distance_description",
                "field_size": "number_of_runners",
            },
        )
        .addfield(
            "is_handicap",
            lambda rec: "HANDICAP" in rec["title"].upper()
            or "H'CAP" in rec["title"].upper(),
            # getattr(
            #     RaceTitle.parse(rec["title"])["race_designation"], "name", None
            # )
            # == "HANDICAP",
            index=4,
        )
        .addfield(
            "datetime",
            lambda rec: build_datetime(rec["date"], rec["off_time"]),
            index=1,
        )
        .addfield("obstacle", lambda rec: parse_obstacle(rec["title"]), index=5)
        .addfield(
            "code", lambda rec: parse_code(rec["obstacle"], rec["title"]), index=6
        )
        .addfield("source", "theracingapi")
        .convert({
            "course": lambda x: x.replace(" (AW)", ""),
            "prize": lambda x: x.replace(",", ""),
            "race_grade": lambda x: str(RaceGrade(x)) if x else None,
            "race_class": lambda x: int(RaceClass(x)),
            "age_restriction": lambda x: generate_min_max(x, "age") if x else None,
            "rating_restriction": lambda x: generate_min_max(x, "rating") if x else None,
            "distance_description": lambda x: str(
                RaceDistance(f"{int(float(x) // 1)}f {int((float(x) % 1) * 220)}y")
            ),
        })
        .addfield("racecourse", lambda rec: {
            "name": rec["course"], 
            "country": rec["region"],
            "surface": rec["surface"], 
            "code": rec["code"], 
            "obstacle": rec["obstacle"],
            "references": {"theracingapi": rec["course"]},
            "source": "theracingapi"
        })
        .convert(
            "runners",
            lambda x, rec: [
                # 
                transform_horse_data(petl.fromdicts([h]), pendulum.parse(rec["datetime"]), rec["obstacle"], rec["surface"])
                for h in x
            ],
            pass_row=True,
        )
        .cutout("course", "surface", "code", "obstacle", "region", "type", "date", "off_time")
        .dicts()
    )
    return [Declaration(**race) for race in race_dicts]

# @task(tags=["RapidAPI"])
def validate_horse_data(data: petl.Table) -> petl.transform.validation.ProblemsView:
    header = (
        "horse",
        "age",
        "sex",
        "sex_code",
        "colour",
        "region",
        "dam",
        "sire",
        "damsire",
        "trainer",
        "owner",
        "number",
        "draw",
        "headgear",
        "lbs",
        "ofr",
        "jockey",
        "last_run",
        "form",
    )

    constraints = [
        {"name": "horse_str", "field": "horse", "test": str},
        {"name": "age_int", "field": "age", "test": int},
        {"name": "sex_str", "field": "sex", "test": str},
        {"name": "colour_str", "field": "colour", "test": str},
        {"name": "region_str", "field": "region", "test": str},
        {"name": "dam_str", "field": "dam", "test": str},
        {"name": "sire_str", "field": "sire", "test": str},
        {"name": "damsire_str", "field": "damsire", "test": str},
        {"name": "trainer_str", "field": "trainer", "test": str},
        {"name": "owner_str", "field": "owner", "test": str},
        {"name": "jockey_str", "field": "jockey", "test": str},
        {"name": "number_digit", "field": "number", "test": lambda x: x.isdigit()},
        {"name": "draw_digit", "field": "draw", "test": lambda x: x.isdigit()},
        {"name": "headgear_str", "field": "headgear", "test": str},
        {"name": "lbs_valid", "field": "lbs", "assertion": validate_weight},
        {"name": "ofr_digit", "field": "ofr", "test": lambda x: x.isdigit()},
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


# @task(tags=["RapidAPI"])
def validate_races_data(data: petl.Table) -> petl.transform.validation.ProblemsView:
    header = (
        "course",
        "date",
        "off_time",
        "race_name",
        "distance_f",
        "region",
        "pattern",
        "race_class",
        "type",
        "age_band",
        "rating_band",
        "prize",
        "field_size",
        "going",
        "surface",
        "runners",
    )

    constraints = [
        {"name": "course_str", "field": "course", "test": str},
        {"name": "date_valid", "field": "date", "assertion": validate_date},
        {"name": "off_time_valid", "field": "off_time", "assertion": validate_time},
        {"name": "distance_float", "field": "distance_f", "test": float},
        {"name": "region_str", "field": "region", "test": str},
        {"name": "pattern_valid", "field": "pattern", "assertion": validate_pattern},
        {
            "name": "race_class_valid",
            "field": "race_class",
            "assertion": lambda x: not x or x.replace("Class ", "").isdigit(),
        },
        {
            "name": "age_band_valid",
            "field": "age_band",
            "assertion": lambda x: x[0].isdigit(),
        },
        {
            "name": "rating_band_valid",
            "field": "rating_band",
            "assertion": lambda x: not x
            or (x[0].isdigit() and x[-1].isdigit() and "-" in x),
        },
        {
            "name": "prize_valid",
            "field": "prize",
            "assertion": lambda x: not x
            or ((x[0] == "£" or x[0] == "€") and x[1].isdigit()),
        },
        {
            "name": "runners_list",
            "field": "runners",
            "assertion": lambda x: all(validate_horse_data(h) for h in x),
        },
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


class TheRacingApiTransformer(Transformer[Declaration]):
    def __init__(self, source_data: petl.Table):
        super().__init__(
            source_data=source_data,
            validator=validate_races_data,
            transformer=transform_races_data,
        )  

if __name__ == "__main__":
    data = TheRacingApiTransformer().transform()  # type: ignore
    print(data)
