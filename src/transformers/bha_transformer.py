import datetime
import sys
from pathlib import Path
from typing import Literal, cast

from models.mongo_racecourse import ObstacleType

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import petl  # type: ignore
from horsetalk import Gender, Horse  # type: ignore
from peak_utility.listish import compact
from pydantic_extra_types.pendulum_dt import Date

from helpers import horse_name_to_pre_mongo_horse
from models import (
    BHAPerfFigsRecord,
    BHARatingsRecord,
    HistoricRatingRecord,
    PreMongoHorse,
)
from models.bha_shared_types import PerfFig


class PreMongoHorseWithHistoricRatings(PreMongoHorse):
    historic_ratings: list[HistoricRatingRecord] | None = None


def transform_historic_rating(
    perf_fig: PerfFig, races_ago: int, date_before: datetime.date
) -> HistoricRatingRecord | None:
    if perf_fig == "-":
        return None

    code, val = perf_fig.split(":")
    if val == "x":
        val = None

    surface: Literal["Turf", "All Weather"] = "All Weather" if code == "A" else "Turf"
    obstacle: ObstacleType | None = (
        cast(ObstacleType, "Hurdle")
        if code == "H"
        else cast(ObstacleType, "Chase")
        if code == "C"
        else None
    )

    return HistoricRatingRecord(
        rating=int(val) if val is not None else None,
        date_before=cast(Date, pendulum.instance(date_before)),
        races_before=races_ago,
        surface=surface,
        obstacle=obstacle,
    )


def transform_perf_figs(record: BHAPerfFigsRecord) -> PreMongoHorseWithHistoricRatings:
    data = petl.fromdicts([record.model_dump()])

    used_fields = (
        "date",
        "racehorse",
        "yof",
        "sex",
        "latest",
        "two_runs_ago",
        "three_runs_ago",
        "four_runs_ago",
        "five_runs_ago",
        "six_runs_ago",
    )
    transformed_record = (
        petl.cut(data, used_fields)
        .rename({"yof": "year", "racehorse": "name"})
        .addfield(
            "country", lambda rec: x.name if (x := Horse(rec["name"]).country) else None
        )
        .addfield(
            "historic_ratings",
            lambda rec: compact(
                [
                    transform_historic_rating(rec["latest"], 0, rec["date"]),
                    transform_historic_rating(rec["two_runs_ago"], 1, rec["date"]),
                    transform_historic_rating(rec["three_runs_ago"], 2, rec["date"]),
                    transform_historic_rating(rec["four_runs_ago"], 3, rec["date"]),
                    transform_historic_rating(rec["five_runs_ago"], 4, rec["date"]),
                    transform_historic_rating(rec["six_runs_ago"], 5, rec["date"]),
                ]
            ),
        )
        .convert(
            {
                "sex": lambda x: Gender[x].sex.name[0],  # type: ignore[misc]
                "name": lambda x: Horse(x).name,
            }
        )  # type: ignore
        .dicts()[0]
    )
    return PreMongoHorseWithHistoricRatings(**transformed_record)


def transform_ratings(record: BHARatingsRecord) -> PreMongoHorse:
    data = petl.fromdicts([record.model_dump()])

    used_fields = (
        "date",
        "name",
        "year",
        "sex",
        "sire",
        "dam",
        "trainer",
        "flat_rating",
        "awt_rating",
        "chase_rating",
        "hurdle_rating",
    )
    rating_types = ["flat", "aw", "chase", "hurdle"]
    transformed_record = (
        petl.cut(data, used_fields)
        .rename({x: x.replace("_rating", "").lower() for x in used_fields})
        .rename({"awt": "aw"})
        .convert({"year": int, "flat": int, "aw": int, "chase": int, "hurdle": int})
        .addfield(
            "country", lambda rec: x.name if (x := Horse(rec["name"]).country) else None
        )
        .addfield(
            "gelded_from",
            lambda rec: pendulum.instance(rec["date"])
            if rec["sex"] == "GELDING"
            else None,
        )
        .convert(
            {
                "sex": lambda x: Gender[x].sex.name[0],  # type: ignore[misc]
                "name": lambda x: Horse(x).name,
                "sire": lambda x: horse_name_to_pre_mongo_horse(
                    x, sex="M", default_country="GB"
                ),
                "dam": lambda x: horse_name_to_pre_mongo_horse(
                    x, sex="F", default_country="GB"
                ),
            }
        )  # type: ignore
        .addfield("ratings", lambda rec: {rtg: rec[rtg] for rtg in rating_types})
        .cutout(*rating_types)
        .dicts()[0]
    )
    return PreMongoHorse(**transformed_record)


if __name__ == "__main__":
    print("Cannot run bha_transformer.py as a script.")
