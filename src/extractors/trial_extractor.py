import tomllib
from prefect import flow, get_run_logger, task


with open("settings.toml", "rb") as f:
    settings = tomllib.load(f)


@flow
def trial_extractor():
    logger = get_run_logger()
    for key in settings:
        logger.info(key)


if __name__ == "__main__":
    trial_extractor()
