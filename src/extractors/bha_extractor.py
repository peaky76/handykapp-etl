# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from helpers import fetch_content, write_file, get_last_occurrence_of
from prefect import flow, task
import pendulum
import yaml


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["source"]["dir"]
FILES = api_info["bha"]["source"]["files"]
DESTINATION = api_info["bha"]["spaces"]["dir"]
UPDATE_DAY = pendulum.TUESDAY
LAST_UPDATE_STR = str(get_last_occurrence_of(UPDATE_DAY)).replace("-", "")


@task(tags=["BHA"], task_run_name="fetch_bha_{data}")
def fetch(data):
    return fetch_content(f"{SOURCE}{FILES[data]}")


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
