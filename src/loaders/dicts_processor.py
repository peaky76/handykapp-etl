import petl
from helpers import log_validation_problem
from prefect import get_run_logger

from loaders.race_processor import race_processor


def dicts_processor():
    logger = get_run_logger()
    logger.info("Starting file processor")
    reject_count = 0
    transform_count = 0

    r = race_processor()
    next(r)

    try:
        while True:
            dicts, validator, transformer, filename, source = yield
            data = petl.fromdicts(dicts)
            problems = validator(data)

            if len(problems.dicts()) > 0:
                logger.warning(f"Validation problems in {filename}")
                if len(problems.dicts()) > 10:
                    logger.warning("Too many problems to log")
                else:
                    for problem in problems.dicts():
                        log_validation_problem(problem)
                reject_count += 1
            else:
                results = transformer(data)
                for race in results:
                    r.send((data, source))

                transform_count += 1
                if transform_count % 10 == 0:
                    logger.info(f"Read {transform_count} races")

    except GeneratorExit:
        logger.info(
            f"Finished transforming {transform_count} races, rejected {reject_count}"
        )
        r.close()
