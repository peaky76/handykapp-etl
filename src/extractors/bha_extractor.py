# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

import pendulum
import tomllib
from helpers import fetch_content, get_last_occurrence_of, write_file
from prefect import flow, task

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["bha"]["source_dir"]
FILES = settings["bha"]["files"]
DESTINATION = settings["bha"]["spaces_dir"]
UPDATE_DAY = pendulum.TUESDAY
LAST_UPDATE_STR = str(get_last_occurrence_of(UPDATE_DAY)).replace("-", "")


@task(tags=["BHA"], task_run_name="fetch_bha_{data}")
def fetch(data):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.76 Safari/537.36"
    }
    return fetch_content(f"{SOURCE}{FILES[data]}", headers=headers)


@task(tags=["BHA"], task_run_name="save_bha_{data}")
def save(data, content):
    filename = f"{DESTINATION}bha_{data}_{LAST_UPDATE_STR}.csv"
    write_file(content, filename)


@flow
def bha_extractor():
    for data in FILES:
        content = fetch(data)
        save(data, content)


if __name__ == "__main__":
    bha_extractor()  # type: ignore
