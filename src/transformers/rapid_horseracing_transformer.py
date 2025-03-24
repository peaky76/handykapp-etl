# To allow running as a script
import sys
from enum import Enum
from pathlib import Path
from typing import cast

from models import MongoRace, PreMongoRunner

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import petl  # type: ignore
import tomllib
from horsetalk import (  # type: ignore
    AWGoingDescription,
    HorseAge,
    Horselength,
    RaceWeight,
)

from transformers.parsers import (
    parse_code,
    parse_horse,
    parse_obstacle,
)

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["rapid_horseracing"]["spaces_dir"]


def transform_horse(
    data, race_date=pendulum.now(), finishing_time=None
) -> PreMongoRunner:
    return (
        petl.rename(
            data,
            {
                "id_horse": "rapid_id",
                "weight": "lbs_carried",
                "last_ran_days_ago": "days_since_prev_run",
                "number": "saddlecloth",
                "OR": "official_rating",
                "distance_beaten": "beaten_distance",
                "position": "finishing_position",
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
        .convert("beaten_distance", lambda x: str(Horselength(x)) if x else None)
        .addfield(
            "finishing_time",
            lambda rec: finishing_time if rec["finishing_position"] == 1 else None,
            index=-1,
        )
        .addfield("official_position", lambda rec: rec["finishing_position"])
        .cutout("horse", "age")
        .dicts()[0]
    )


def transform_results(data) -> MongoRace:
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
        .convert(
            "datetime",
            lambda x: pendulum.from_format(x, "YYYY-MM-DD HH:mm:ss").isoformat(),
        )
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
            lambda rec: (
                "AW"
                if any(
                    x.name in rec["going_description"].upper()
                    for x in cast(type[Enum], AWGoingDescription)
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
        .addfield(
            "runners",
            lambda rec: [
                transform_horse(
                    petl.fromdicts([h]),
                    race_date=pendulum.parse(rec["datetime"]),
                    finishing_time=rec["finish_time"],
                )
                for h in rec["horses"]
            ],
        )
        .cutout("horses", "finish_time")
        .dicts()
    )


if __name__ == "__main__":
    print("Cannot run rapid_horseracing_transformer.py as a script.")
