# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from typing import List

import pendulum
import petl  # type: ignore
import tomllib
from horsetalk import AWGoingDescription, HorseAge, RaceWeight  # type: ignore
from models import Result, Run

from transformers.parsers import (
    parse_code,
    parse_horse,
    parse_obstacle,
)
from transformers.theracingapi_transformer import generate_min_max
from transformers.transformer import Transformer
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


def transform_horse_data(data: petl.Table, race_date=pendulum.now(), finishing_time=None) -> Run:
    horse = (
        petl.rename(
            data,
            {
                "id_horse": "rapid_id",
                "weight": "lbs_carried",
                "number": "saddlecloth",
                "OR": "official_rating",
            },
        )
        .convert({
            "age": int,
            "last_ran_days_ago": int,
            "official_rating": int,
            "non_runner": lambda x: bool(int(x)),
            "lbs_carried": lambda x: int(RaceWeight(x).lb),
        })
        .addfield(
            "year",
            lambda rec: HorseAge(rec["age"], context_date=race_date)._official_dob.year,
            index=1,
        )
        .addfield("country", lambda rec: parse_horse(rec["horse"], "GB")[1], index=1)
        .addfield("name", lambda rec: parse_horse(rec["horse"])[0], index=0)        
        .convert("sire", lambda x: {
            "name": parse_horse(x)[0],
            "country": parse_horse(x, "GB")[1],
            "sex": "M",
            "source": "rapid"
        } if x else None)
        .convert("dam", lambda x: {
            "name": parse_horse(x)[0],
            "country": parse_horse(x, "GB")[1],
            "sex": "F",
            "source": "rapid"
        } if x else None)
        .convert("jockey", lambda x: {"name": x, "role": "jockey", "references": {"rapid": x}})
        .convert("trainer", lambda x: {"name": x, "role": "trainer", "references": {"rapid": x}})
        .addfield("prev_run", lambda rec: race_date.subtract(days=rec["last_ran_days_ago"]) if rec["last_ran_days_ago"] else None)
        .addfield(
            "finishing_time",
            lambda rec: finishing_time if rec["position"] == 1 else None,
            index=-1,
        )
        .addfield("source", "rapid")
        .cutout("horse", "age")
        .dicts()[0]
    )
    return Run(**horse)


def transform_results_data(data: petl.Table) -> List[Result]:
    results_dicts = (
        petl.rename(
            data,
            {
                "date": "datetime",
                "age": "age_restriction",
                "canceled": "is_cancelled",
                "class": "race_class",
                "distance": "distance_description",
                "going": "going_description",
            },
        )
        .convert({
            "datetime": lambda x: pendulum.parse(x).isoformat(),
            "finished": lambda x: bool(int(x)),
            "is_cancelled": lambda x: bool(int(x)),
            "age_restriction": lambda x: generate_min_max(x, "age") if x else None,
        })
        .addfield(
            "is_handicap",
            lambda rec: "HANDICAP" in rec["title"].upper()
            or "H'CAP" in rec["title"].upper(),
            index=4,
        )
        .addfield("obstacle", lambda rec: parse_obstacle(rec["title"]), index=5)
        .addfield(
            "surface",
            lambda rec: (
                "AW"
                if any(
                    x.name in rec["going_description"].upper()
                    for x in AWGoingDescription
                )
                else "Turf"
                # TODO: Reinstate when Horsetalk is updated (needs prefect to update to pendulum > 3)
                # TODO: Handle mixed meetings as multiparse returns a list and only first used here
                # Going(rec["going_description"]).surface.name.title()
                # if "COURSE" not in rec["going_description"].upper()
                # else next(iter(Going.multiparse(rec["going_description"]).values())).surface.name.title()
                # )
            ),
        )
        .addfield(
            "code", lambda rec: parse_code(rec["obstacle"], rec["title"]), index=6
        )
        .addfield("racecourse", lambda rec: {
            "name": rec["course"],
            "country": "GB" if "£" in rec["prize"] else "IRE",
            "surface": rec["surface"],
            "code": rec["code"],
            "obstacle": rec["obstacle"],
            "references": {"rapid": rec["course"]},
            "source": "rapid"
        })
        .addfield(
            "runners",
            lambda rec: [
                transform_horse_data(
                    petl.fromdicts([h]),
                    race_date=pendulum.parse(rec["datetime"]),
                    finishing_time=rec["finish_time"],
                )
                for h in rec["horses"]
            ],
        )
        .addfield("references", lambda rec: {"rapid": rec["id_race"]})
        .addfield("source", "rapid")
        .cutout("surface", "code", "obstacle", "course", "horses", "finish_time", "id_race")
        .dicts()
    )
    return [Result(**res) for res in results_dicts]


def validate_horse(data: petl.Table) -> petl.transform.validation.ProblemsView:
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


def validate_results_data(data: petl.Table) -> petl.transform.validation.ProblemsView:
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
        {
            "name": "going_valid",
            "field": "going",
            "test": lambda x: validate_going(x, allow_empty=True),
        },
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

class RapidHorseracingTransformer(Transformer[Result]):
    def __init__(self, source_data: petl.Table):
        super().__init__(
            source_data=source_data,
            validator=validate_results_data,
            transformer=transform_results_data,
        )  

if __name__ == "__main__":
    data = RapidHorseracingTransformer().transform()  # type: ignore
    print(data)
