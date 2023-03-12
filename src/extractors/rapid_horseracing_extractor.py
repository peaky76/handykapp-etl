# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

import json
import pendulum
from time import sleep
from prefect import flow, task
from helpers import fetch_content, get_files, read_file, write_file
from prefect.blocks.system import Secret
import yaml

with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

# NB: In RapidAPI, results are called race details

SOURCE = api_info["rapid_horseracing"]["source"]["dir"]
DESTINATION = api_info["rapid_horseracing"]["spaces"]["dir"]
RACECARDS_DESTINATION = f"{DESTINATION}racecards/"
RESULTS_DESTINATION = f"{DESTINATION}results/"
LIMITS = api_info["rapid_horseracing"]["limits"]


def get_file_date(filename):
    return filename.split(".")[0][-8:]


def get_headers(url):
    return {
        "x-rapidapi-host": url.split("//")[1].split("/")[0],
        "x-rapidapi-key": Secret.load("rapid-api-key").get(),
    }


@task(tags=["Rapid"])
def get_unfetched_race_ids(last_checked):
    racecard_files = list(get_files(RACECARDS_DESTINATION, last_checked))
    return [race["id_race"] for file in racecard_files for race in read_file(file)]


@task(tags=["Rapid"], task_run_name="extract_result_{race_id}")
def extract_result(race_id):
    source = f"{SOURCE}race/{race_id}"
    headers = get_headers(source)

    content = fetch_content(source, headers=headers)
    filename = f"{RESULTS_DESTINATION}rapid_api_result_{race_id}.json"
    write_file(content, filename)


@task(tags=["Rapid"], task_run_name="extract_racecards_{date}")
def extract_racecards(date):  # date - YYYY-MM-DD
    source = f"{SOURCE}racecards"
    params = {"date": date}
    headers = get_headers(source)

    content = fetch_content(source, params, headers)
    date_str = date.replace("-", "")
    filename = f"{RACECARDS_DESTINATION}rapid_api_racecards_{date_str}.json"
    write_file(content, filename)


@task(tags=["Rapid"])
def get_next_racecard_date():
    start_date = pendulum.parse("2020-01-01")
    end_date = pendulum.now()
    test_date = start_date

    files = list(get_files(RACECARDS_DESTINATION))
    file_date_strs = [get_file_date(filename) for filename in files]
    file_dates = [pendulum.from_format(x, "YYYYMMDD") for x in file_date_strs]

    while test_date <= end_date:
        if test_date not in file_dates:
            return test_date.format("YYYY-MM-DD")
        test_date += pendulum.duration(days=1)

    return None


@flow
def update_results_to_do_list():
    filename = f"{DESTINATION}results_to_do_list.json"
    current_status = read_file(filename)
    last_checked = (
        pendulum.parse(current_status["last_checked"])
        if current_status["last_checked"]
        else None
    )

    result_files = list(get_files(RESULTS_DESTINATION))
    new_race_ids = get_unfetched_race_ids(last_checked)
    done_race_ids = [filename.split(".")[0].split("_")[-1] for filename in result_files]
    to_do_race_ids = current_status["results_to_do"] + new_race_ids

    content = json.dumps(
        {
            "last_checked": str(pendulum.now()),
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
    races_batch = read_file(f"{DESTINATION}results_to_do_list.json")["results_to_do"][
        : LIMITS["day"] - 2
    ]
    for race_id in races_batch:
        extract_result(race_id)
        sleep(90)


if __name__ == "__main__":
    rapid_horseracing_extractor()
