import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import petl  # type: ignore
from horsetalk import Gender, Horse  # type: ignore

from helpers import horse_name_to_pre_mongo_horse
from models import BHARatingsRecord, PreMongoHorse


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
        .addfield("country", lambda rec: Horse(rec["name"]).country.name)
        .addfield(
            "gelded_from",
            lambda rec: pendulum.instance(rec["date"])
            if rec["sex"] == "GELDING"
            else None,
        )
        .convert(
            {
                "sex": lambda x: Gender[x].sex.name[0],
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
