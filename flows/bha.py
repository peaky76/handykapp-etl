from dateutil import relativedelta as rd
from datetime import date
from helpers import fetch_content, write_file
from prefect import flow, task


SOURCE = "https://www.britishhorseracing.com/feeds/v4/ratings/csv/"
DESTINATION = "handykapp/bha/"
FILES = {
    "ratings": "ratings.csv",
    "rating_changes": "ratings.csv?diff",
    "perf_figs": "performance-figures.csv",
}

UPDATE_DAY = 1  # Tuesday
TODAY = date.today()
LAST_UPDATE_DATE = TODAY + rd.relativedelta(days=-6, weekday=UPDATE_DAY)
LAST_UPDATE_STR = str(LAST_UPDATE_DATE).replace('-', '')


@task(tags=["BHA"], task_run_name="fetch_bha_{data}")
def fetch(data):
    return fetch_content(f"{SOURCE}{FILES[data]}")


@task(tags=["BHA"], task_run_name="save_bha_{data}")
def save(data, content):
    filename = (f"{DESTINATION}bha_{data}_{LAST_UPDATE_STR}.csv")
    write_file(content, filename)


@flow
def bha_extractor():
    for data in FILES:
        content = fetch(data)
        save(data, content)


if __name__ == "__main__":
    bha_extractor()
