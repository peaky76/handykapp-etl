# To allow running as a script
from pathlib import Path
import sys
import pendulum

sys.path.append(str(Path(__file__).resolve().parent.parent))

import yaml
from helpers.helpers import fetch_content, write_file
from prefect import flow, task
from prefect.blocks.system import Secret

with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["theracingapi"]["source"]["dir"]
DESTINATION = api_info["theracingapi"]["spaces"]["dir"]


def get_headers():
    return {
        "x-rapidapi-host": "the-racing-api1.p.rapidapi.com",
        "x-rapidapi-key": Secret.load("rapid-api-key").get(),
    }


@task(tags=["TheRacingApi"])
def extract_countries():
    source = f"{SOURCE}courses/regions"
    headers = get_headers()

    return fetch_content(source, headers=headers)


@task(tags=["TheRacingApi"])
def extract_racecards(day="tomorrow", region_codes=["gb", "ire"]):
    source = f"{SOURCE}racecards/basic"
    headers = get_headers()
    params = {"day": day, "region_codes": region_codes}

    content = fetch_content(source, params=params, headers=headers)
    date_str = pendulum.now().add(days=1).format("YYYYMMDD")
    filename = f"{DESTINATION}racecards/theracingapi_racecards_{date_str}.json"
    write_file(content, filename)


@flow
def theracingapi_racecards_extractor():
    extract_racecards()


@flow
def theracingapi_countries_extractor():
    extract_countries()


if __name__ == "__main__":
    theracingapi_racecards_extractor()
    theracingapi_countries_extractor()
