import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from flows.helpers import fetch_content, write_file, get_last_occurrence_of
from prefect import flow, task


@flow
def bha_transformer():
    pass
