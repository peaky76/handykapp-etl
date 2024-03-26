import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from typing import List

import petl  # type: ignore
import tomllib
from helpers import get_files, stream_file
from horsetalk import Gender
from models import Horse
from prefect import task

from transformers.parsers import parse_horse
from transformers.transformer import Transformer
from transformers.validators import (
    validate_horse,
    validate_rating,
    validate_sex,
    validate_year,
)

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["bha"]["spaces_dir"]


@task(tags=["BHA"], task_run_name="get_{date}_{csv_type}_csv")
def get_csv(csv_type="ratings", date="latest"):
    idx = -1 if date == "latest" else 0
    search_string = "" if date == "latest" else date
    csvs = [
        csv
        for csv in list(get_files(SOURCE))
        if csv_type in csv and search_string in csv
    ]
    return csvs[idx] if csvs else None


@task(tags=["BHA"])
def read_csv(csv):
    source = petl.MemorySource(stream_file(csv))
    return petl.fromcsv(source)


@task(tags=["BHA"])
def transform_ratings_data(data: petl.Table) -> List[Horse]:
    used_fields = (
        "Name",
        "Year",
        "Sex",
        "Sire",
        "Dam",
        "Trainer",
        "Flat rating",
        "AWT rating",
        "Chase rating",
        "Hurdle rating",
    )
    rating_types = ["flat", "aw", "chase", "hurdle"]
    horse_dicts = ( 
        petl.cut(data, used_fields)
        .rename({x: x.replace(" rating", "").lower() for x in used_fields})
        .rename({"awt": "aw"})
        .convert({"year": int, "flat": int, "aw": int, "chase": int, "hurdle": int})
        .addfield("name_and_country", lambda rec: parse_horse(rec["name"], "GB"))
        .addfield("sire_name_and_country", lambda rec: parse_horse(rec["sire"], "GB"))
        .addfield("dam_name_and_country", lambda rec: parse_horse(rec["dam"], "GB"))
        .cutout("name", "sire", "dam")
        .unpack("name_and_country", ["name", "country"])
        .addfield("sire", lambda rec: {
            "name": rec["sire_name_and_country"][0],
            "country": rec["sire_name_and_country"][1],
            "sex": "M",
            "source": "bha"
        })
        .addfield("dam", lambda rec: {
            "name": rec["dam_name_and_country"][0],
            "country": rec["dam_name_and_country"][1],
            "sex": "F",
            "source": "bha"
        })
        .addfield(
            "operations",
            lambda rec: [{"operation_type": "gelding", "date": None}]
            if rec["sex"] == "GELDING"
            else None,
        )
        .convert("trainer", lambda x: {"name": x, "role": "trainer", "references": {"bha": x}, "source": "bha"})
        .convert({"sex": lambda x: Gender[x].sex.name[0]})  # type: ignore
        .addfield("ratings", lambda rec: {rtg: rec[rtg] for rtg in rating_types})
        .cutout(*rating_types, "sire_name_and_country", "dam_name_and_country")
        .addfield("source", "bha")
        .dicts()
    )
    return [Horse(**horse) for horse in horse_dicts]


@task(tags=["BHA"])
def validate_ratings_data(data: petl.Table) -> petl.transform.validation.ProblemsView:
    header = (
        "Name",
        "Year",
        "Sex",
        "Sire",
        "Dam",
        "Trainer",
        "Flat rating",
        "Diff Flat",
        "Flat Clltrl",
        "AWT rating",
        "Diff AWT",
        "AWT Clltrl",
        "Chase rating",
        "Diff Chase",
        "Chase Clltrl",
        "Hurdle rating",
        "Diff Hurdle",
        "Hurdle Clltrl",
    )
    constraints = [
        {"name": "name_str", "field": "Name", "assertion": validate_horse},
        {"name": "year_valid", "field": "Year", "assertion": validate_year},
        {"name": "sex_valid", "field": "Sex", "assertion": validate_sex},
        {"name": "sire_str", "field": "Sire", "assertion": validate_horse},
        {"name": "dam_str", "field": "Dam", "assertion": validate_horse},
        {"name": "trainer_str", "field": "Trainer", "test": str},
        {"name": "flat_valid", "field": "Flat rating", "assertion": validate_rating},
        {"name": "awt_valid", "field": "AWT rating", "assertion": validate_rating},
        {"name": "chase_valid", "field": "Chase rating", "assertion": validate_rating},
        {
            "name": "hurdle_rating_int",
            "field": "Hurdle rating",
            "assertion": validate_rating,
        },
    ]
    validator = {"header": header, "constraints": constraints}
    return petl.validate(data, **validator)


class BHATransformer(Transformer[Horse]):
    def __init__(self):
        super().__init__(
            source_data=read_csv(get_csv()),
            validator=validate_ratings_data,
            transformer=transform_ratings_data,
        )

if __name__ == "__main__":
    data = BHATransformer().transform()  # type: ignore
    print(data)
