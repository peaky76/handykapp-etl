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
def fetch_bha_ratings():
    return fetch_content(f'{RATINGS_CSVS_URL}/ratings.csv')


@task(tags=["BHA"])
def fetch_bha_rating_changes():
    return fetch_content(f'{RATINGS_CSVS_URL}/ratings.csv?diff')


@task(tags=["BHA"])
def fetch_bha_performance_figures():
    return fetch_content(f'{RATINGS_CSVS_URL}/performance-figures.csv')


@task(tags=["BHA"])
def save_bha_ratings(content):
    filename = f'{BASE_DESTINATION}/bha_ratings_{LAST_UPDATE_STR}.csv'
    write_file(content, filename)


@task(tags=["BHA"])
def save_bha_rating_changes(content):
    filename = f'{BASE_DESTINATION}/bha_rating_changes_{LAST_UPDATE_STR}.csv'
    write_file(content, filename)


@task(tags=["BHA"])
def save_bha_performance_figures(content):
    filename = f'{BASE_DESTINATION}/bha_perf_figs_{LAST_UPDATE_STR}.csv'
    write_file(content, filename)


@flow
def bha_extractor():
    bha_ratings = fetch_bha_ratings()
    save_bha_ratings(bha_ratings)
    bha_rating_changes = fetch_bha_rating_changes()
    save_bha_rating_changes(bha_rating_changes)
    bha_performance_figures = fetch_bha_performance_figures()
    save_bha_performance_figures(bha_performance_figures)


if __name__ == "__main__":
    bha_extractor()
