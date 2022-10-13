from dateutil import relativedelta as rd
from datetime import date
from helpers import fetch_content, write_file
from prefect import flow, task


BASE_URL = 'https://www.britishhorseracing.com'
RATINGS_CSVS_URL = f'{BASE_URL}/feeds/v4/ratings/csv'
BASE_DESTINATION = 'handykapp/bha'

UPDATE_DAY = 1  # Tuesday
TODAY = date.today()
LAST_UPDATE_DATE = TODAY + rd.relativedelta(weeks=-1, weekday=UPDATE_DAY)
LAST_UPDATE_STR = str(LAST_UPDATE_DATE).replace('-', '')


@task(tags=["BHA"])
def extract_bha_ratings():
    content = fetch_content(f'{RATINGS_CSVS_URL}/ratings.csv')
    filename = f'{BASE_DESTINATION}/bha_ratings_{LAST_UPDATE_STR}.csv'
    write_file(content, filename)


@task(tags=["BHA"])
def extract_bha_rating_changes():
    content = fetch_content(f'{RATINGS_CSVS_URL}/ratings.csv?diff')
    filename = f'{BASE_DESTINATION}/bha_rating_changes_{LAST_UPDATE_STR}.csv'
    write_file(content, filename)


@task(tags=["BHA"])
def extract_bha_performance_figures():
    content = fetch_content(f'{RATINGS_CSVS_URL}/performance-figures.csv')
    filename = f'{BASE_DESTINATION}/bha_perf_figs_{LAST_UPDATE_STR}.csv'
    write_file(content, filename)


@flow
def bha_extractor():
    extract_bha_ratings()
    extract_bha_rating_changes()
    extract_bha_performance_figures()


if __name__ == "__main__":
    bha_extractor()
