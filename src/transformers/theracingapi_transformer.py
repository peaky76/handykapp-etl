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
    validate_class,
    validate_date,
    validate_distance,
    validate_going,
    validate_prize,
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
def transform_racecard(data):
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


# def validate_horse(data):
#     header = (
#         "horse",
#         "id_horse",
#         "jockey",
#         "trainer",
#         "age",
#         "weight",
#         "number",
#         "last_ran_days_ago",
#         "non_runner",
#         "form",
#         "position",
#         "distance_beaten",
#         "owner",
#         "sire",
#         "dam",
#         "OR",
#         "sp",
#         "odds",
#     )
#     constraints = [
#         dict(name="horse_str", field="horse", test=str),
#         dict(name="id_horse_int", field="id_horse", test=int),
#         dict(name="jockey_str", field="jockey", test=str),
#         dict(name="trainer_str", field="trainer", test=str),
#         dict(name="age_int", field="age", test=int),
#         dict(name="weight_valid", field="weight", assertion=validate_weight),
#         dict(name="number_int", field="number", test=int),
#         dict(name="last_ran_days_ago_int", field="last_ran_days_ago", test=int),
#         dict(name="non_runner_bool", field="non_runner", test=bool),
#         dict(name="form_str", field="form", test=str),
#         dict(name="position_int", field="position", test=int),
#         dict(name="distance_beaten_str", field="distance_beaten", test=str),
#         dict(name="owner_str", field="owner", test=str),
#         dict(name="sire_str", field="sire", test=str),
#         dict(name="dam_str", field="dam", test=str),
#         dict(name="OR_int", field="OR", test=int),
#         dict(name="sp_float", field="sp", assertion=float),
#         dict(name="odds_list", field="odds", test=list),
#     ]
#     validator = {"header": header, "constraints": constraints}
#     return petl.validate(data, **validator)


# @task(tags=["Rapid"])
# def validate_results(data):
#     header = (
#         "id_race",
#         "course",
#         "date",
#         "title",
#         "distance",
#         "age",
#         "going",
#         "finished",
#         "canceled",
#         "finish_time",
#         "prize",
#         "class",
#         "horses",
#     )
#     constraints = [
#         dict(name="course_str", field="course", test=str),
#         dict(name="date_valid", field="date", assertion=validate_date),
#         dict(name="title_str", field="title", test=str),
#         dict(name="distance_valid", field="distance", assertion=validate_distance),
#         dict(name="age_int", field="age", test=int),
#         dict(name="going_valid", field="going", assertion=validate_going),
#         dict(name="finished_bool", field="finished", test=bool),
#         dict(name="canceled_bool", field="canceled", test=bool),
#         dict(name="prize_valid", field="prize", assertion=validate_prize),
#         dict(name="class_int", field="class", assertion=validate_class),
#         dict(
#             name="horses_list",
#             field="horses",
#             assertion=lambda x: [validate_horse(h) for h in x],
#         ),
#     ]
#     validator = {"header": header, "constraints": constraints}
#     return petl.validate(data, **validator)


# @flow
# def rapid_horseracing_transformer():
#     logger = get_run_logger()
#     results = []
#     count = 0

#     for file in get_files(f"{SOURCE}results"):
#         count += 1
#         result = read_file(file)
#         results.append(result)
#         if count % 100 == 0:
#             logger.info(f"Read {count} results")

#     data = petl.fromdicts(results)
#     problems = validate_results(data)
#     for problem in problems.dicts():
#         log_validation_problem(problem)

#     return transform_results(data)


# if __name__ == "__main__":
#     data = rapid_horseracing_transformer()  # type: ignore
#     print(data)
