# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from helpers import read_file
from prefect import flow
import yaml


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["rapid_horseracing"]["spaces"]["dir"]


@flow
def rapid_horseracing_transformer():
    return read_file(f"{SOURCE}results/rapid_api_result_187431.json")


if __name__ == "__main__":
    data = rapid_horseracing_transformer()
    print(data)
