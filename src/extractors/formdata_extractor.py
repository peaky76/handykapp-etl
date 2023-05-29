# To allow running as a script
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from prefect import flow, task


@task(tags=["Racing Research"])
def fetch():
    pass


@flow(tags=["Racing Research"])
def formdata_extractor():
    fetch()


if __name__ == "__main__":
    formdata_extractor()  # type: ignore
