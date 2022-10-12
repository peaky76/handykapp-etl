from datetime import date, datetime, timedelta
from dotenv import load_dotenv
from helpers import download_file, get_files, write_file
from os import getenv
from prefect import flow, task
from time import sleep


load_dotenv()
BASE_URL = 'https://horse-racing.p.rapidapi.com'
BASE_DESTINATION = 'handykapp/rapid_horseracing'
LIMITS = {'day': 50, 'minute': 10}


def get_headers(url):
    return {
        "x-rapidapi-host": url.split("//")[1].split("/")[0],
        "x-rapidapi-key": getenv('RAPID_API_KEY')
    }


@task(tags=["Rapid"])
def download_rapid_racecards(date):  # date - YYYY-MM-DD
    source = f'{BASE_URL}/racecards'
    params = {'date': date}
    headers = get_headers(source)

    content = download_file(source, params, headers)
    date_str = date.replace('-', '')
    filename = f"{BASE_DESTINATION}/rapid_api_racecards_{date_str}.json"
    write_file(content, filename)


@task(tags=["Rapid"])
def get_next_racecard_date():
    start_date = date.fromisoformat('2020-01-01')
    end_date = date.today()
    test_date = start_date

    files = get_files(BASE_DESTINATION)
    file_date_strs = [name.split('.')[0][-8:] for name in files]
    file_dates = [datetime.strptime(x, '%Y%m%d').date() for x in file_date_strs]

    while test_date <= end_date:
        if test_date not in file_dates:
            return test_date.strftime('%Y-%m-%d')
        test_date += timedelta(days=1)

    return None


@flow
def rapid_horseracing_fetcher():
    count = 10
    while count < LIMITS['day']:
        date = get_next_racecard_date()
        download_rapid_racecards(date)
        count += 1
        sleep(60 // LIMITS['minute'])


if __name__ == "__main__":
    rapid_horseracing_fetcher()
