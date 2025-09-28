# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow

from loaders.theracingapi_loader import load_theracingapi_data


@flow
def database_runner():
    load_theracingapi_data()  # type: ignore


if __name__ == "__main__":
    database_runner()  # type: ignore
