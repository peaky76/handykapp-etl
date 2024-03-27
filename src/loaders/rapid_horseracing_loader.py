# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


import tomllib
from helpers import get_files
from prefect import flow, get_run_logger
from processors.rapid_file_processor import RapidFileProcessor

from .loader import Loader

with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)

SOURCE = settings["rapid_horseracing"]["spaces_dir"]


# @flow
# def load_rapid_horseracing_data():
#     logger = get_run_logger()
#     logger.info("Starting rapid_horseracing loader")

#     p = RecordProcessor()
#     r = p.start()
#     next(r)

#     files = get_files(f"{SOURCE}results")

#     for file in files:
#         if file != "results_to_do_list.json":
#             record = read_file(file)
#             r.send((record, validate_results, transform_results, file, "rapid"))

#     r.close()


# if __name__ == "__main__":
#     load_rapid_horseracing_data()

@flow
def load_rapid_horseracing_data(*, from_date=None):
    loader = Loader(get_files(f"{SOURCE}results"), RapidFileProcessor())
    loader.load()


if __name__ == "__main__":
    load_rapid_horseracing_data()