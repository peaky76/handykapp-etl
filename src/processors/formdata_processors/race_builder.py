from prefect import get_run_logger


def race_builder():
    logger = get_run_logger()

    try:
        while True:
            horse = yield

    except GeneratorExit:
        pass
