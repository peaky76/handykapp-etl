# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

import yaml
from helpers.helpers import fetch_content, write_file
from prefect import task
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
