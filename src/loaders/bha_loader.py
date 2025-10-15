# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import datetime

import pendulum
import petl
import tomllib
from prefect import flow, get_run_logger, task

from clients import SpacesClient
from clients import mongo_client as client
from models.bha_ratings_record import BHARatingsRecord
from processors import ratings_processor
from transformers.bha_transformer import transform_ratings

db = client.handykapp

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)
SOURCE = settings["bha"]["spaces_dir"]  # Directory where BHA CSV files are stored


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


def convert_header_to_field_name(header: str) -> str:
    return header.strip().lower().replace(" ", "_")


def csv_row_to_dict(header_row, data_row):
    return dict(zip(header_row, data_row))


@flow
def load_bha_data():
    logger = get_run_logger()
    logger.info("Starting BHA loader")

    r = ratings_processor()
    next(r)

    csv = get_csv()
    logger.info(f"Got CSV file: {csv}")

    data = read_csv(csv)
    date_str = csv.split("_")[-1].split(".")[0]  # Remove file extension
    pendulum_date = pendulum.from_format(date_str, "YYYYMMDD")
    date = datetime.date(pendulum_date.year, pendulum_date.month, pendulum_date.day)

    rows = list(data)
    logger.info(f"Total rows in CSV: {len(rows)}")

    header = [convert_header_to_field_name(col) for col in rows[0]]

    try:
        for data_row in rows[1:]:
            row_dict = csv_row_to_dict(header, data_row)
            record = BHARatingsRecord(**row_dict, date=date)
            transformed_ratings = transform_ratings(record)
            r.send(transformed_ratings)
    except Exception as e:
        logger.error(f"Unable to process BHA ratings {csv}: {e}")

    r.close()


if __name__ == "__main__":
    load_bha_data()
