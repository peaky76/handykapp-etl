import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import petl  # type: ignore
import tomllib
from horsetalk import Gender, Horse  # type: ignore
from prefect import flow, task

from clients import SpacesClient
from models import BHARatingsRecord, PreMongoHorse

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["bha"]["spaces_dir"]


@task(tags=["BHA"], task_run_name="get_{date}_{csv_type}_csv")
def get_csv(csv_type="ratings", date="latest"):
    idx = -1 if date == "latest" else 0
    search_string = "" if date == "latest" else date
    csvs = [
        csv
        for csv in list(SpacesClient.get_files(SOURCE))
        if csv_type in csv and search_string in csv
    ]
    return csvs[idx] if csvs else None


@task(tags=["BHA"])
def read_csv(csv):
    source = petl.MemorySource(SpacesClient.stream_file(csv))
    return petl.fromcsv(source)


@task(tags=["BHA"])
def transform_ratings(
    record: BHARatingsRecord, date_time: pendulum.DateTime
) -> PreMongoHorse:
    data = petl.fromdicts([record.model_dump()])

    used_fields = (
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
            lambda rec: date_time.date() if rec["sex"] == "GELDING" else None,
        )
        .convert(
            {"sex": lambda x: Gender[x].sex.name[0], "name": lambda x: Horse(x).name}
        )  # type: ignore
        .addfield("ratings", lambda rec: {rtg: rec[rtg] for rtg in rating_types})
        .cutout(*rating_types)
        .dicts()[0]
    )
    return PreMongoHorse(**transformed_record)


def convert_header_to_field_name(header: str) -> str:
    return header.strip().lower().replace(" ", "_")


def csv_row_to_dict(header_row, data_row):
    return dict(zip(header_row, data_row))


@flow
def bha_transformer():
    csv = get_csv()
    data = read_csv(csv)

    rows = list(data)
    header = [convert_header_to_field_name(col) for col in rows[0]]

    for data_row in rows[1:]:
        row_dict = csv_row_to_dict(header, data_row)
        record = BHARatingsRecord(**row_dict)


if __name__ == "__main__":
    data = bha_transformer()  # type: ignore
    print(data)
