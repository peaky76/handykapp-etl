from prefect import flow, task
from requests import get

BASE_URL = 'https://www.britishhorseracing.com'
RATINGS_CSVS_URL = f'{BASE_URL}/feeds/v4/ratings/csv'

BASE_DESTINATION = 'files/bha'


@task
def download_file(url):
    response = get(url)
    print(response.status_code)
    return response.content


@task
def write_file(content, filename):
    with open(filename, 'wb+') as f:
        f.write(content)


@flow
def download_bha_ratings():
    content = download_file(f'{RATINGS_CSVS_URL}/ratings.csv')
    write_file(content, f'{BASE_DESTINATION}/bha_ratings_20221004.csv')


@flow
def download_bha_rating_changes():
    content = download_file(f'{RATINGS_CSVS_URL}/ratings.csv?diff')
    write_file(content, f'{BASE_DESTINATION}/bha_rating_changes_20221004.csv')


@flow
def download_bha_performance_figures():
    content = download_file(f'{RATINGS_CSVS_URL}/performance-figures.csv')
    write_file(content, f'{BASE_DESTINATION}/bha_perf_figs_20221004.csv')


@flow
def bha_scraper():
    download_bha_ratings()
    download_bha_rating_changes()
    download_bha_performance_figures()


if __name__ == "__main__":
    bha_scraper()
