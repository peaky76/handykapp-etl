import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import petl  # type: ignore
from horsetalk import Gender, Horse  # type: ignore
from prefect import task

from models import BHARatingsRecord, PreMongoHorse


@task(tags=["BHA"])
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
            lambda rec: rec["date"] if rec["sex"] == "GELDING" else None,
        )
        .convert(
            {"sex": lambda x: Gender[x].sex.name[0], "name": lambda x: Horse(x).name}
        )  # type: ignore
        .addfield("ratings", lambda rec: {rtg: rec[rtg] for rtg in rating_types})
        .cutout(*rating_types)
        .dicts()[0]
    )
    return PreMongoHorse(**transformed_record)


if __name__ == "__main__":
    print("Cannot run bha_transformer.py as a script.")
