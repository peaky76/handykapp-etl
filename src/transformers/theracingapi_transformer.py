# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


import pendulum
import petl  # type: ignore
import tomllib
from helpers import get_files, log_validation_problem, read_file
from horsetalk import (  # type: ignore
    CoatColour,
    Gender,
    Headgear,
    HorseAge,
    RaceClass,
    RaceDistance,
    RaceGrade,
)
from prefect import flow, get_run_logger, task

from transformers.parsers import parse_code, parse_obstacle
from transformers.validators import (
    validate_date,
    validate_time,
    validate_weight,
)

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["theracingapi"]["spaces_dir"]


def build_datetime(date_str: str, time_str: str) -> str:
    hour, minute = time_str.split(":")
    hour = h + 12 if (h := int(hour)) < 11 else h
    return pendulum.parse(f"{date_str} {hour}:{minute}").to_iso8601_string()


def transform_horse(data, race_date=pendulum.now()):
    return (
        petl.rename(
            data,
            {
                "horse": "name",
                "region": "country",
                "number": "saddlecloth",
                "lbs": "lbs_carried",
                "ofr": "official_rating",
            },
        )
        .convert(
            {
                "name": lambda x: x.upper(),
                "sex": lambda x: Gender[x].sex.name[0],
                "age": int,
                "colour": lambda x: CoatColour[x].name.title(),
                "sire": lambda x: x.upper(),
                "dam": lambda x: x.upper(),
                "damsire": lambda x: x.upper(),
                "saddlecloth": int,
                "draw": int,
                "lbs_carried": int,
                "headgear": lambda x: Headgear[x].name.title() if x else None,
                "official_rating": int,
            }
        )
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
        .convert("jockey", lambda x: x.split("(")[0].strip())
        .cutout("sex_code", "last_run", "form", "age")
        .dicts()[0]
    )


@task(tags=["TheRacingAPI"])
def transform_races(data):
    return (
        petl.rename(
            data,
            {
                "race_name": "title",
                "age_band": "age_restriction",
                "rating_band": "rating_restriction",
                "going": "going_description",
                "pattern": "race_grade",
                "distance_f": "distance_description",
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
        .convert(
            {
                "course": lambda x: x.replace(" (AW)", ""),
                "prize": lambda x: x.replace(",", ""),
                "race_grade": lambda x: str(RaceGrade(x)) if x else None,
                "race_class": lambda x: int(RaceClass(x)),
                "age_restriction": lambda x: x or None,
                "rating_restriction": lambda x: x or None,
                "distance_description": lambda x: str(
                    RaceDistance(f"{int(float(x) // 1)}f {int((float(x) % 1) * 220)}y")
                ),
                "runners": lambda x: [transform_horse(petl.fromdicts([h])) for h in x],
            }
        )
        .cutout("field_size", "region", "type", "date", "off_time")
        .dicts()
    )


def validate_horse(data):
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


@task(tags=["TheRacingAPI"])
def validate_races(data):
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
        {"name": "pattern_valid", "field": "pattern", "test": RaceGrade},
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
            "assertion": lambda x: (x[0] == "£" or x[0] == "€") and x[1].isdigit(),
        },
        {
            "name": "runners_list",
            "field": "runners",
            "assertion": lambda x: [validate_horse(h) for h in x],
        },
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@task(tags=["TheRacingAPI"])
def read_racecards():
    logger = get_run_logger()
    racecards = []
    count = 0

    for file in get_files(f"{SOURCE}racecards"):
        count += 1
        day = read_file(file)
        racecards.extend(day["racecards"])
        if count % 50 == 0:
            logger.info(f"Read {count} days of racecards")

    logger.info(f"Read {count} days of racecards")
    return racecards


def theracingapi_transformer():
    logger = get_run_logger()
    logger.info("Starting theracingapi transformer")
    transform_count = 0

    try:
        while True:
            file = yield
            day = read_file(file)
            data = petl.fromdicts(day["racecards"])
            transform_count += 1
            if transform_count % 50 == 0:
                logger.info(f"Read {transform_count} days of racecards")
                
    except GeneratorExit:
        logger.info(f"Finished transforming {transform_count} days of racecards")    

@flow
def theracingapi_transformer_old():
    races = read_racecards()
    data = petl.fromdicts(races)
    problems = validate_races(data)
    for problem in problems.dicts():
        log_validation_problem(problem)

    return transform_races(data)


if __name__ == "__main__":
    data = theracingapi_transformer_old()  # type: ignore
    print(data)
