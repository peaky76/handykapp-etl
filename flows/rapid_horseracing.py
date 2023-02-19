import json
import pytz
from datetime import date, datetime, timedelta
from time import sleep
from prefect import flow, task
from flows.helpers import fetch_content, get_files, read_file, write_file
from prefect.blocks.system import Secret

# NB: In RapidAPI, results are called race details

BASE_URL = "https://horse-racing.p.rapidapi.com/"
BASE_DESTINATION = "handykapp/rapid_horseracing/"
RACECARDS_DESTINATION = f"{BASE_DESTINATION}racecards/"
RESULTS_DESTINATION = f"{BASE_DESTINATION}results/"
LIMITS = {"day": 50, "minute": 10}


def get_file_date(filename):
    return filename.split(".")[0][-8:]


def get_headers(url):
    return {
        "x-rapidapi-host": url.split("//")[1].split("/")[0],
        "x-rapidapi-key": Secret.load("rapid-api-key").get(),
    }


@task(tags=["Rapid"], task_run_name="extract_result_{race_id}")
def extract_result(race_id):
    source = f"{BASE_URL}race/{race_id}"
    headers = get_headers(source)

    content = fetch_content(source, headers=headers)
    filename = f"{RESULTS_DESTINATION}rapid_api_result_{race_id}.json"
    write_file(content, filename)


@task(tags=["Rapid"], task_run_name="extract_racecards_{date}")
def extract_racecards(date):  # date - YYYY-MM-DD
    source = f"{BASE_URL}racecards"
    params = {"date": date}
    headers = get_headers(source)

    content = fetch_content(source, params, headers)
    date_str = date.replace("-", "")
    filename = f"{RACECARDS_DESTINATION}rapid_api_racecards_{date_str}.json"
    write_file(content, filename)


@task(tags=["Rapid"])
def get_race_ids(file):
    return [race["id_race"] for race in read_file(file)]


@task(tags=["Rapid"])
def get_next_racecard_date():
    start_date = date.fromisoformat("2020-01-01")
    end_date = date.today()
    test_date = start_date

    files = get_files(RACECARDS_DESTINATION)
    file_date_strs = [get_file_date(filename) for filename in files]
    file_dates = [datetime.strptime(x, "%Y%m%d").date() for x in file_date_strs]

    while test_date <= end_date:
        if test_date not in file_dates:
            return test_date.strftime("%Y-%m-%d")
        test_date += timedelta(days=1)

    return None


@flow
def update_results_to_do_list():
    filename = f"{BASE_DESTINATION}results_to_do_list.json"
    current_status = read_file(filename)
    last_checked = (
        datetime.strptime(current_status["last_checked"], "%Y-%m-%d %H:%M:%S.%f%z")
        if current_status["last_checked"]
        else None
    )

    racecard_files = get_files(RACECARDS_DESTINATION, last_checked)
    result_files = get_files(RESULTS_DESTINATION)

    new_race_ids = [
        race_id for file in racecard_files for race_id in get_race_ids(file)
    ]
    done_race_ids = [filename.split(".")[0].split("_")[-1] for filename in result_files]
    to_do_race_ids = current_status["results_to_do"] + new_race_ids

    content = json.dumps(
        {
            "last_checked": str(datetime.now(pytz.utc)),
            "results_to_do": [
                race_id for race_id in to_do_race_ids if race_id not in done_race_ids
            ],
            "results_done": done_race_ids,
        }
    )
    write_file(content, filename)


@flow
def rapid_horseracing_extractor():
    # Add another day"s racing to the racecards folder
    date = get_next_racecard_date()
    extract_racecards(date)

    # Update the list of results to fetch
    update_results_to_do_list()

    # Fetch a number of results within the limits presented
    races_batch = read_file(f"{BASE_DESTINATION}results_to_do_list.json")[
        "results_to_do"
    ][: LIMITS["day"] - 2]
    for race_id in races_batch:
        extract_result(race_id)
        sleep(90)


if __name__ == "__main__":
    rapid_horseracing_extractor()
