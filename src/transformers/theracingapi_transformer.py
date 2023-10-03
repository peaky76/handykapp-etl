# To allow running as a script
from pathlib import Path
import sys


sys.path.append(str(Path(__file__).resolve().parent.parent))


import pendulum
import petl  # type: ignore
import yaml
from horsetalk import CoatColour, Gender, RaceClass, RaceDistance, RaceGrade
from helpers import log_validation_problem, read_file, get_files
from prefect import flow, get_run_logger, task
from transformers.parsers import (
    parse_handicap,
    parse_obstacle,
    yob_from_age,
)
from transformers.validators import (
    validate_date,
    validate_time,
    validate_weight,
)


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["theracingapi"]["spaces"]["dir"]


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
                "headgear": lambda x: x or None,
                "official_rating": int,
            }
        )
        .addfield("year", lambda rec: yob_from_age(rec["age"], race_date), index=3)
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
def transform_racecards(data):
    return (
        petl.rename(
            data,
            {
                "off_time": "time",
                "race_name": "title",
                "age_band": "age_restriction",
                "rating_band": "rating_restriction",
                "going": "going_description",
                "pattern": "grade",
                "distance_f": "distance_description",
                "race_class": "class",
            },
        )
        .addfield("is_handicap", lambda rec: parse_handicap(rec["title"]), index=4)
        .addfield("obstacle", lambda rec: parse_obstacle(rec["title"]), index=5)
        .convert(
            {
                "time": lambda x: pendulum.parse(
                    f"{str(int(x.split(':')[0])+12)}:{x.split(':')[1]}",
                    tz="Europe/London",
                ).to_time_string(),
                "prize": lambda x: x.replace(",", ""),
                "grade": lambda x: str(RaceGrade(x)) if x else None,
                "class": lambda x: int(RaceClass(x)),
                "distance_description": lambda x: str(
                    RaceDistance(f"{int(float(x) // 1)}f {int((float(x) % 1) * 220)}y")
                ),
                "runners": lambda x: [transform_horse(petl.fromdicts([h])) for h in x],
            }
        )
        .cutout("field_size", "region", "type")
        .dicts()[0]
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
        dict(name="horse_str", field="horse", test=str),
        dict(name="age_int", field="age", test=int),
        dict(name="sex_str", field="sex", test=str),
        dict(name="colour_str", field="colour", test=str),
        dict(name="region_str", field="region", test=str),
        dict(name="dam_str", field="dam", test=str),
        dict(name="sire_str", field="sire", test=str),
        dict(name="damsire_str", field="damsire", test=str),
        dict(name="trainer_str", field="trainer", test=str),
        dict(name="owner_str", field="owner", test=str),
        dict(name="jockey_str", field="jockey", test=str),
        dict(name="number_digit", field="number", test=lambda x: x.isdigit()),
        dict(name="draw_digit", field="draw", test=lambda x: x.isdigit()),
        dict(name="headgear_str", field="headgear", test=str),
        dict(name="lbs_valid", field="lbs", assertion=validate_weight),
        dict(name="ofr_digit", field="ofr", test=lambda x: x.isdigit()),
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@task(tags=["TheRacingAPI"])
def validate_racecards(data):
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
        dict(name="course_str", field="course", test=str),
        dict(name="date_valid", field="date", assertion=validate_date),
        dict(name="off_time_valid", field="off_time", assertion=validate_time),
        dict(name="distance_float", field="distance_f", test=float),
        dict(name="region_str", field="region", test=str),
        dict(name="pattern_valid", field="pattern", test=RaceGrade),
        dict(
            name="race_class_valid",
            field="race_class",
            assertion=lambda x: not x or x.replace("Class ", "").isdigit(),
        ),
        dict(
            name="age_band_valid", field="age_band", assertion=lambda x: x[0].isdigit()
        ),
        dict(
            name="rating_band_valid",
            field="rating_band",
            assertion=lambda x: not x
            or (x[0].isdigit() and x[-1].isdigit() and "-" in x),
        ),
        dict(
            name="prize_valid",
            field="prize",
            assertion=lambda x: (x[0] == "£" or x[0] == "€") and x[1].isdigit(),
        ),
        dict(
            name="runners_list",
            field="runners",
            assertion=lambda x: [validate_horse(h) for h in x],
        ),
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@flow
def theracingapi_transformer():
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

    data = petl.fromdicts(racecards)
    problems = validate_racecards(data)
    for problem in problems.dicts():
        log_validation_problem(problem)

    return transform_racecards(data)


if __name__ == "__main__":
    data = theracingapi_transformer()  # type: ignore
    print(data)
