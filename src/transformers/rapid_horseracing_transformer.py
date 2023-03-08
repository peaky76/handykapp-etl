# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from helpers import log_validation_problem, read_file, stream_file
from prefect import flow, task
from transformers.parsers import (
    parse_going,
    parse_horse,
    parse_weight,
    parse_yards,
    yob_from_age,
)
import pendulum
import petl
import re
import yaml


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["rapid_horseracing"]["spaces"]["dir"]


def validate_date(date):
    try:
        pendulum.parse(date)
        return True
    except pendulum.exceptions.ParserError:
        return False
    except TypeError:
        return False


def validate_distance(distance):
    pattern = r"^(([1-4]m)(\s)?([1-7]f)?|([4-7]f))$"
    return bool(re.match(pattern, distance)) if distance else False


def validate_going(going):
    if not going:
        return False

    goings = [
        "HARD",
        "FIRM",
        "GOOD TO FIRM",
        "GOOD",
        "GOOD TO SOFT",
        "SOFT",
        "SOFT TO HEAVY",
        "HEAVY",
        "STANDARD",
        "STANDARD TO SLOW",
        "SLOW",
        "YIELDING",
    ]
    going = going.upper().replace(")", "").replace(" IN PLACES", "").split(" (")
    return going[0] in goings and (going[1] in goings if len(going) == 2 else True)


def validate_prize(prize):
    pattern = r"^[£|\$][0-9]{1,3}(,)*[0-9]{1,3}$"
    return bool(re.match(pattern, prize)) if prize else False


def validate_weight(weight):
    pattern = r"^[0-9]{1,2}-[0-1][0-9]$"
    return bool(re.match(pattern, weight)) if weight else False


def transform_horse(horse_data, race_date=pendulum.now()):
    return (
        petl.rename(
            horse_data,
            {
                "id_horse": "rapid_id",
                "weight": "lbs_carried",
                "last_ran_days_ago": "days_since_prev_run",
                "number": "saddlecloth",
                "OR": "official_rating",
            },
        )
        .convert(
            {
                "age": int,
                "days_since_prev_run": int,
                "official_rating": int,
                "non_runner": lambda x: bool(int(x)),
                "lbs_carried": parse_weight,
            }
        )
        .addfield("year", lambda rec: yob_from_age(rec["age"], race_date), index=1)
        .addfield("country", lambda rec: parse_horse(rec["horse"])[1], index=1)
        .addfield("name", lambda rec: parse_horse(rec["horse"])[0], index=0)
        .addfield("sire_country", lambda rec: parse_horse(rec["sire"])[1], index=-4)
        .convert("sire", lambda x: parse_horse(x)[0])
        .addfield("dam_country", lambda rec: parse_horse(rec["dam"])[1], index=-3)
        .convert("dam", lambda x: parse_horse(x)[0])
        .cutout("horse", "age")
        .dicts()[0]
    )


@task(tags=["Rapid"])
def transform_result(data):
    return (
        petl.rename(
            data, {"id_race": "rapid_id", "date": "datetime", "age": "age_restriction"}
        )
        .convert("finished", lambda x: bool(int(x)))
        .convert("canceled", lambda x: bool(int(x)))
        .convert(
            "distance",
            lambda x: {
                "description": x,
                "official_yards": int(parse_yards(x)),
                "actual_yards": None,
            },
        )
        .convert(
            "going",
            lambda x: {
                "description": x,
                "official_main": parse_going(x)["main"],
                "official_secondary": parse_going(x)["secondary"],
            },
        )
        .addfield(
            "result",
            lambda rec: [
                transform_horse(petl.fromdicts([h]), pendulum.parse(rec["datetime"]))
                for h in rec["horses"]
            ],
        )
        .cutout("horses")
        .dicts()
    )


@task(tags=["Rapid"])
def validate_horse(data):
    header = (
        "horse",
        "id_horse",
        "jockey",
        "trainer",
        "age",
        "weight",
        "number",
        "last_ran_days_ago",
        "non_runner",
        "form",
        "position",
        "distance_beaten",
        "owner",
        "sire",
        "dam",
        "OR",
        "sp",
        "odds",
    )
    constraints = [
        dict(name="horse_str", field="horse", test=str),
        dict(name="id_horse_int", field="id_horse", test=int),
        dict(name="jockey_str", field="jockey", test=str),
        dict(name="trainer_str", field="trainer", test=str),
        dict(name="age_int", field="age", test=int),
        dict(name="weight_valid", field="weight", assertion=validate_weight),
        dict(name="number_int", field="number", test=int),
        dict(name="last_ran_days_ago_int", field="last_ran_days_ago", test=int),
        dict(name="non_runner_bool", field="non_runner", test=bool),
        dict(name="form_str", field="form", test=str),
        dict(name="position_int", field="position", test=int),
        dict(name="distance_beaten_str", field="distance_beaten", test=str),
        dict(name="owner_str", field="owner", test=str),
        dict(name="sire_str", field="sire", test=str),
        dict(name="dam_str", field="dam", test=str),
        dict(name="OR_int", field="OR", test=int),
        dict(name="sp_float", field="sp", assertion=float),
        dict(name="odds_list", field="odds", test=list),
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@task(tags=["Rapid"])
def validate_result(data):
    header = (
        "id_race",
        "course",
        "date",
        "title",
        "distance",
        "age",
        "going",
        "finished",
        "canceled",
        "finish_time",
        "prize",
        "class",
        "horses",
    )
    constraints = [
        dict(name="course_str", field="course", test=str),
        dict(name="date_valid", field="date", assertion=validate_date),
        dict(name="title_str", field="title", test=str),
        dict(name="distance_valid", field="distance", assertion=validate_distance),
        dict(name="age_int", field="age", test=int),
        dict(name="going_valid", field="going", assertion=validate_going),
        dict(name="finished_bool", field="finished", test=bool),
        dict(name="canceled_bool", field="canceled", test=bool),
        dict(name="prize_valid", field="prize", assertion=validate_prize),
        dict(name="class_int", field="class", test=int),
        dict(
            name="horses_list",
            field="horses",
            assertion=lambda x: [validate_horse(h) for h in x],
        ),
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@flow
def rapid_horseracing_transformer():
    json = read_file(f"{SOURCE}results/rapid_api_result_187686.json")
    data = petl.fromdicts([json])
    problems = validate_result(data)
    for problem in problems.dicts():
        log_validation_problem(problem)
    return transform_result(data)


if __name__ == "__main__":
    data = rapid_horseracing_transformer()
    print(data)
