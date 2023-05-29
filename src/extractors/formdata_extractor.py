# To allow running as a script
from pathlib import Path
import re
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, task


def is_dist_going(string: str) -> str:
    dist_going_regex = r"\d\.?\d?[H|F|M|G|D|S|V|f|g|d|s]"
    return bool(re.match(dist_going_regex, string))


def is_race_date(string: str) -> str:
    date_regex = r"\d{1,2}[A-Z][a-z]{2}\d{2}"
    return bool(re.match(date_regex, string))


@task(tags=["Racing Research"])
def fetch():
    pass


@flow
def formdata_extractor():
    fetch()


if __name__ == "__main__":
    formdata_extractor()  # type: ignore
