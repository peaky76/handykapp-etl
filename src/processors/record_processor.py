from concurrent.futures import ThreadPoolExecutor
from threading import Lock

from prefect import get_run_logger

from processors.race_processor import race_processor


def transform_single_record(record, transformer, filename, logger):
    try:
        return transformer(record)
    except Exception as e:
        logger.error(f"Error transforming {filename}: {e}")
        return None


def record_processor():
    logger = get_run_logger()
    logger.info("Starting record processor")
    reject_count_lock = Lock()
    transform_count_lock = Lock()
    reject_count = 0
    transform_count = 0

    r = race_processor()
    next(r)

    # Create thread pool for parallel transformations
    with ThreadPoolExecutor(max_workers=4) as executor:
        try:
            while True:
                record, transformer, filename, source = yield

                # Submit transformation to thread pool
                future = executor.submit(
                    transform_single_record, record, transformer, filename, logger
                )
                results = future.result()

                if not results:
                    with reject_count_lock:
                        reject_count += 1
                    continue

                for race in results:
                    try:
                        if race.code == "Flat":
                            r.send((race, source))
                            with transform_count_lock:
                                transform_count += 1
                                if transform_count % 25 == 0:
                                    logger.info(
                                        f"Read {transform_count} races. Current: {race.datetime} at {race.course}"
                                    )
                    except Exception as e:  # noqa: PERF203
                        logger.error(
                            f"Error during processing of race in {filename}: {e}"
                        )
                        with reject_count_lock:
                            reject_count += 1
                        continue

        except GeneratorExit:
            logger.info(
                f"Finished transforming {transform_count} races, rejected {reject_count}"
            )
            r.close()
