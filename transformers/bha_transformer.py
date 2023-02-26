import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from flows.helpers import get_files, read_file
from prefect import flow, task
import pendulum
import yaml


with open("api_info.yml", "r") as f:
    api_info = yaml.load(f, Loader=yaml.loader.SafeLoader)

SOURCE = api_info["bha"]["space_dir"]


@task(tags=["BHA"], task_run_name="get_ratings_files")
def get_ratings_files(date=None):
    files = [file for file in get_files(SOURCE) if "ratings" in file]
    if date:
        files = [
            file for file in files if pendulum.parse(date).format("YYYYMMDD") in file
        ]
    return files


@flow
def bha_transformer():
    data = read_file(f"{SOURCE}bha_ratings_20230221.csv")
    print(data)


if __name__ == "__main__":
    bha_transformer()
