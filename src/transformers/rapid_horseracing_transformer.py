# To allow running as a script
import sys
from datetime import timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import petl  # type: ignore
import tomllib
from helpers import get_files, log_validation_problem, read_file
from horsetalk import AWGoingDescription, Going, HorseAge, RaceWeight  # type: ignore
from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

from transformers.parsers import (
    parse_code,
    parse_horse,
    parse_obstacle,
)
from transformers.validators import (
    validate_class,
    validate_date,
    validate_distance,
    validate_going,
    validate_prize,
    validate_weight,
)

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["rapid_horseracing"]["spaces_dir"]


def transform_horses(horse_data, race_date=pendulum.now(), finishing_time=None):
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
                "lbs_carried": lambda x: RaceWeight(x).lb,
            }
        )
        .addfield(
            "year",
            lambda rec: HorseAge(rec["age"], context_date=race_date)._official_dob.year,
            index=1,
        )
        .addfield("country", lambda rec: parse_horse(rec["horse"], "GB")[1], index=1)
        .addfield("name", lambda rec: parse_horse(rec["horse"])[0], index=0)
        .addfield(
            "sire_country", lambda rec: parse_horse(rec["sire"], "GB")[1], index=-4
        )
        .convert("sire", lambda x: parse_horse(x)[0])
        .addfield("dam_country", lambda rec: parse_horse(rec["dam"], "GB")[1], index=-3)
        .convert("dam", lambda x: parse_horse(x)[0])
        .addfield(
            "finishing_time",
            lambda rec: finishing_time if rec["position"] == 1 else None,
            index=-1,
        )
        .cutout("horse", "age")
        .dicts()[0]
    )


@task(
    tags=["Rapid"],
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=7),
)
def transform_results(data):
    return (
        petl.rename(
            data,
            {
                "id_race": "rapid_id",
                "date": "datetime",
                "age": "age_restriction",
                "canceled": "cancelled",
                "distance": "distance_description",
                "going": "going_description",
            },
        )
        .convert("datetime", lambda x: pendulum.parse(x).isoformat())
        .convert("finished", lambda x: bool(int(x)))
        .convert("cancelled", lambda x: bool(int(x)))
        .addfield(
            "is_handicap",
            lambda rec: "HANDICAP" in rec["title"].upper()
            or "H'CAP" in rec["title"].upper(),
            index=4,
        )
        .addfield("obstacle", lambda rec: parse_obstacle(rec["title"]), index=5)
        .addfield(
            "surface",
            # TODO: Fix in horsetalk as will incorrectly categorise some turf races as AW
            # but otherwise whole pipeline fails
            lambda rec: "AW"
            if "AW" in rec["going_description"]
            or Going(
                rec["going_description"].replace(" (", ", ").replace(")", "")
            ).primary
            in AWGoingDescription
            else "Turf",
            index=6,
        )
        .addfield(
            "code", lambda rec: parse_code(rec["obstacle"], rec["title"]), index=6
        )
        .addfield(
            "result",
            lambda rec: [
                transform_horses(
                    petl.fromdicts(rec["horses"]),
                    race_date=pendulum.parse(rec["datetime"]),
                    finishing_time=rec["finish_time"],
                )
            ]
            if rec["horses"]
            else [],
        )
        .cutout("horses", "finish_time")
        .dicts()
    )


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
        {"name": "horse_str", "field": "horse", "test": str},
        {"name": "id_horse_int", "field": "id_horse", "test": int},
        {"name": "jockey_str", "field": "jockey", "test": str},
        {"name": "trainer_str", "field": "trainer", "test": str},
        {"name": "age_int", "field": "age", "test": int},
        {"name": "weight_valid", "field": "weight", "assertion": validate_weight},
        {"name": "number_int", "field": "number", "test": int},
        {"name": "last_ran_days_ago_int", "field": "last_ran_days_ago", "test": int},
        {"name": "non_runner_bool", "field": "non_runner", "test": bool},
        {"name": "form_str", "field": "form", "test": str},
        {"name": "position_int", "field": "position", "test": int},
        {"name": "distance_beaten_str", "field": "distance_beaten", "test": str},
        {"name": "owner_str", "field": "owner", "test": str},
        {"name": "sire_str", "field": "sire", "test": str},
        {"name": "dam_str", "field": "dam", "test": str},
        {"name": "OR_int", "field": "OR", "test": int},
        {"name": "sp_float", "field": "sp", "assertion": float},
        {"name": "odds_list", "field": "odds", "test": list},
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@task(tags=["Rapid"])
def validate_results(data):
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
        {"name": "course_str", "field": "course", "test": str},
        {"name": "date_valid", "field": "date", "assertion": validate_date},
        {"name": "title_str", "field": "title", "test": str},
        {"name": "distance_valid", "field": "distance", "assertion": validate_distance},
        {"name": "age_int", "field": "age", "test": int},
        {"name": "going_valid", "field": "going", "assertion": validate_going},
        {"name": "finished_bool", "field": "finished", "test": bool},
        {"name": "canceled_bool", "field": "canceled", "test": bool},
        {"name": "prize_valid", "field": "prize", "assertion": validate_prize},
        {"name": "class_int", "field": "class", "assertion": validate_class},
        {
            "name": "horses_list",
            "field": "horses",
            "assertion": lambda x: [validate_horse(h) for h in x],
        },
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


@flow
def rapid_horseracing_transformer():
    logger = get_run_logger()
    results = []
    count = 0

    for file in get_files(f"{SOURCE}results"):
        count += 1
        result = read_file(file)
        results.append(result)
        if count % 100 == 0:
            logger.info(f"Read {count} results")

    data = petl.fromdicts(results)
    problems = validate_results(data)
    for problem in problems.dicts():
        log_validation_problem(problem)

    return transform_results(data)


if __name__ == "__main__":
    data = rapid_horseracing_transformer()  # type: ignore
    print(data)
