import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from helpers import get_files, read_file, stream_file
from prefect import flow, task
import pendulum
import petl
import yaml


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["space_dir"]


def parse_horse(horse):
    split_string = horse.replace(")", "").split(" (")
    name, country = split_string if len(split_string) == 2 else (horse, None)
    return (name, country)


def parse_sex(sex):
    return "M" if sex.upper() in ["GELDING", "COLT", "STALLION"] else "F"


@task(tags=["BHA"])
def get_ratings_files(date=None):
    files = [file for file in get_files(SOURCE) if "ratings" in file]
    if date:
        files = [
            file for file in files if pendulum.parse(date).format("YYYYMMDD") in file
        ]
    return files


@task(tags=["BHA"])
def prune_ratings_csv(csv):
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
    return (
        petl.fromcsv(csv)
        .cut(used_fields)
        .rename({x: x.replace(" rating", "").lower() for x in used_fields})
        .dicts()
    )


@task(tags=["BHA"])
def select_dams(data):
    return sorted(list(set([x["dam"] for x in data])))


@task(tags=["BHA"])
def select_sires(data):
    return sorted(list(set([x["sire"] for x in data])))


@flow
def bha_transformer():
    source = petl.MemorySource(stream_file(get_ratings_files()[-1]))
    data = prune_ratings_csv(source)
    sires = select_sires(data)
    dams = select_dams(data)


if __name__ == "__main__":
    bha_transformer()
