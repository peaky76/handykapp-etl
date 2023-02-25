from flows.helpers import fetch_content, write_file, get_last_occurrence_of
from prefect import flow, task

# To allow running as a script
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))


SOURCE = "https://www.britishhorseracing.com/feeds/v4/ratings/csv/"
DESTINATION = "handykapp/bha/"
FILES = {
    "ratings": "ratings.csv",
    "rating_changes": "ratings.csv?diff",
    "perf_figs": "performance-figures.csv",
}

UPDATE_DAY = 1  # Tuesday
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
    bha_extractor()
