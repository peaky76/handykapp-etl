from datetime import date, datetime, timedelta, timezone
from dotenv import load_dotenv
from helpers import fetch_content, get_files, read_json, write_file
from os import getenv
from prefect import flow, task
from time import sleep


load_dotenv()
BASE_URL = 'https://horse-racing.p.rapidapi.com'
BASE_DESTINATION = 'handykapp/rapid_horseracing'
RACECARDS_DESTINATION = f"{BASE_DESTINATION}/racecards"
RESULTS_DESTINATION = f"{BASE_DESTINATION}/results"
LIMITS = {'day': 50, 'minute': 10}


def get_file_date(filename):
    return filename.split('.')[0][-8:]


def get_headers(url):
    return {
       "x-rapidapi-host": url.split("//")[1].split("/")[0],
       "x-rapidapi-key": getenv('RAPID_API_KEY')
    }

# @task(tags=["Rapid"])
# def race_detail(race_id):
#     source = f'{BASE_URL}/race/{race_id}'
#     destination = ''
#     headers = RapidAPIExtractors._get_headers(source)
#     log_description = f'race {race_id} from Rapid API'
#     return Fetcher(source, destination, headers=headers, log_description=log_description)    

# @task(tags=["Rapid"])
# def fetch_results(date):
#     source = '{BASE_URL}/results'
#     destination = ''
#     headers = RapidAPIExtractors._get_headers(source)
#     params = {'date': date}
#     log_description = f'UK & IRE results for {date} from Rapid API'        
#     return Fetcher(source, destination, headers=headers, log_description=log_description, params=params)


@task(tags=["Rapid"])
def extract_racecards(date):  # date - YYYY-MM-DD
    source = f'{BASE_URL}/racecards'
    params = {'date': date}
    headers = get_headers(source)

    content = fetch_content(source, params, headers)
    date_str = date.replace('-', '')
    filename = f"{RACECARDS_DESTINATION}/rapid_api_racecards_{date_str}.json"
    write_file(content, filename)


@task(tags=["Rapid"])
def get_race_ids(file):
    return [race['id_race'] for race in read_json(file)]


@task(tags=["Rapid"])
def get_next_racecard_date():
    start_date = date.fromisoformat('2020-01-01')
    end_date = date.today()
    test_date = start_date

    files = get_files(RACECARDS_DESTINATION)
    file_date_strs = [get_file_date(filename) for filename in files]
    file_dates = [datetime.strptime(x, '%Y%m%d').date() for x in file_date_strs]

    while test_date <= end_date:
        if test_date not in file_dates:
            return test_date.strftime('%Y-%m-%d')
        test_date += timedelta(days=1)

    return None


@flow
def rapid_horseracing_extractor():
    # Add another day's racing to the racecards folder 
    date = get_next_racecard_date()
    extract_racecards(date)


if __name__ == "__main__":
    rapid_horseracing_extractor()
