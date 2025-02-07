# To allow running as a script
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))


from prefect import flow, get_run_logger


@flow
def load_nothing_and_log():
    logger = get_run_logger()
    logger.info("I've run, but done nothing.")


if __name__ == "__main__":
    load_nothing_and_log()
